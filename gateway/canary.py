"""灰度版本与熔断健康保护的联动：自动回退与分阶段恢复。

- 版本的首选上游熔断（OPEN）时，该版本进入保护态：有效权重压到 0，
  新流量回到其余（稳定）版本；
- 保护持续 protect_seconds 后进入恢复态：按 recovery_steps 分阶段把权重
  加回配置值，每个阶段至少持续 step_seconds，只放少量试探流量；
  试探流量同时充当熔断器半开探测的来源，避免权重为 0 时熔断器永远
  等不到探测请求的死锁；
- 恢复期间首选上游再次熔断（探测失败）立即退回保护态并重新计时；
- 状态推进只由注入时钟与熔断状态驱动，单个迟到的成功结果不会提前
  放开权重；同样输入序列每次运行结果一致。

健康信号：以版本的首选上游（upstreams[0]）熔断状态为准，OPEN 视为不健康；
HALF_OPEN（探测进行中）不视为不健康，让恢复态的试探流量把探测走完。
"""

from __future__ import annotations

import enum
import logging
import threading
from dataclasses import dataclass
from typing import Mapping

from gateway.circuit import CircuitBreakerRegistry, CircuitState
from gateway.clock import Clock, SystemClock
from gateway.models import Route

log = logging.getLogger(__name__)


class CanaryState(enum.Enum):
    NORMAL = "normal"
    PROTECTED = "protected"
    RECOVERING = "recovering"


@dataclass(frozen=True)
class CanaryConfig:
    """灰度联动参数。

    protect_seconds: 熔断后保护态持续时长（应 >= 熔断器 recovery_seconds，
        保证进入恢复态时熔断器已允许半开探测）。
    step_seconds: 恢复态每个权重阶段的最短持续时长。
    recovery_steps: 恢复阶段权重占配置权重的比例序列（末位通常为 1.0）。
    """

    protect_seconds: float = 30.0
    step_seconds: float = 10.0
    recovery_steps: tuple[float, ...] = (0.1, 0.5, 1.0)

    def __post_init__(self) -> None:
        if self.protect_seconds <= 0 or self.step_seconds <= 0:
            raise ValueError("durations must be > 0")
        if not self.recovery_steps or any(s <= 0 or s > 1 for s in self.recovery_steps):
            raise ValueError("recovery_steps must be in (0, 1]")


class _VersionState:
    __slots__ = ("state", "entered_at", "step")

    def __init__(self) -> None:
        self.state = CanaryState.NORMAL
        self.entered_at = 0.0
        self.step = 0


class CanaryController:
    """按 (路由, 版本) 维护灰度联动状态机，输出有效权重。

    状态机在每次 effective_weights 求值时按当前时钟与熔断状态推进，
    纯函数式输入（now, circuit states）保证可复现。
    """

    def __init__(
        self,
        circuits: CircuitBreakerRegistry,
        config: CanaryConfig | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._circuits = circuits
        self._config = config or CanaryConfig()
        self._clock: Clock = clock or SystemClock()
        self._lock = threading.Lock()
        self._states: dict[tuple[str, int], _VersionState] = {}

    def effective_weights(
        self, route: Route, configured: Mapping[int, int]
    ) -> dict[int, int]:
        """根据熔断状态推进状态机，返回各版本当前有效权重。"""
        now = self._clock.now()
        with self._lock:
            return {
                v.version: self._effective(route.route_id, v, configured, now)
                for v in route.versions
            }

    def state_of(self, route_id: str, version: int) -> CanaryState:
        """观测用：该版本当前联动状态（由最近的求值推进）。"""
        with self._lock:
            return self._states.get((route_id, version), _VersionState()).state

    # ---- 内部：状态机求值（调用方须持锁） ----

    def _effective(
        self, route_id: str, version, configured: Mapping[int, int], now: float
    ) -> int:
        key = (route_id, version.version)
        state = self._states.setdefault(key, _VersionState())
        unhealthy = (
            self._circuits.state(version.upstreams[0]) is CircuitState.OPEN
        )
        self._advance(key, state, unhealthy, now)
        target = configured.get(version.version, 0)
        if state.state is CanaryState.NORMAL:
            return target
        if state.state is CanaryState.PROTECTED:
            return 0
        # RECOVERING：按比例放量，配置权重 >0 时至少放 1 保证试探流量存在
        if target <= 0:
            return 0
        fraction = self._config.recovery_steps[state.step]
        return max(1, int(target * fraction))

    def _advance(
        self, key: tuple[str, int], state: _VersionState, unhealthy: bool, now: float
    ) -> None:
        cfg = self._config
        if state.state is CanaryState.NORMAL:
            if unhealthy:
                self._enter(state, CanaryState.PROTECTED, now)
                log.info("canary %s: NORMAL -> PROTECTED (circuit open)", key)
            return
        if state.state is CanaryState.PROTECTED:
            if now - state.entered_at >= cfg.protect_seconds:
                self._enter(state, CanaryState.RECOVERING, now)
                log.info(
                    "canary %s: PROTECTED -> RECOVERING step=0 (protected %.1fs)",
                    key, cfg.protect_seconds,
                )
            return
        # RECOVERING
        if unhealthy:
            self._enter(state, CanaryState.PROTECTED, now)
            log.info("canary %s: RECOVERING -> PROTECTED (probe failed)", key)
            return
        if now - state.entered_at >= cfg.step_seconds:
            state.step += 1
            state.entered_at = now
            if state.step >= len(cfg.recovery_steps):
                self._enter(state, CanaryState.NORMAL, now)
                log.info("canary %s: RECOVERING -> NORMAL (all steps done)", key)
            else:
                log.info("canary %s: RECOVERING advance to step=%d", key, state.step)

    @staticmethod
    def _enter(state: _VersionState, new_state: CanaryState, now: float) -> None:
        state.state = new_state
        state.entered_at = now
        state.step = 0
