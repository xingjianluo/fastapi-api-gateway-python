"""灰度权重与健康保护（熔断）联动：自动回退与分阶段恢复。

- 版本任一上游熔断进入 OPEN -> 该版本权重立即压到 0（SUPPRESSED），不再接新流量；
- 熔断打开满 recovery_seconds 后进入恢复（RECOVERING），按 ramp 比例逐级放量；
- 每个恢复阶段须稳定经过 stage_dwell_seconds 才进入下一级，只由时钟推进，
  迟到的成功结果不会提前放开；
- 恢复期间任一上游再次 OPEN -> 立即退回 SUPPRESSED，重新计时；
- 全部阶段完成回到 NORMAL，恢复基础权重。
"""

from __future__ import annotations

import enum
import logging
import threading
from dataclasses import dataclass

from gateway.circuit import CircuitState
from gateway.clock import Clock, SystemClock

log = logging.getLogger(__name__)


class CanaryMode(enum.Enum):
    NORMAL = "normal"
    SUPPRESSED = "suppressed"
    RECOVERING = "recovering"


@dataclass(frozen=True)
class CanaryConfig:
    """灰度联动参数。

    recovery_seconds: 版本被压零后等待多久开始放试探流量（与熔断器恢复时长一致）。
    stage_dwell_seconds: 每个恢复阶段需要的稳定时长。
    ramp: 恢复各阶段权重占基础权重的比例（升序，如 (0.1, 0.5, 1.0)）。
    """

    recovery_seconds: float
    stage_dwell_seconds: float
    ramp: tuple[float, ...]

    def __post_init__(self) -> None:
        if self.recovery_seconds <= 0 or self.stage_dwell_seconds <= 0:
            raise ValueError("recovery_seconds and stage_dwell_seconds must be > 0")
        if not self.ramp or any(f <= 0 or f > 1 for f in self.ramp):
            raise ValueError("ramp must be non-empty fractions in (0, 1]")
        if tuple(sorted(self.ramp)) != tuple(self.ramp):
            raise ValueError("ramp must be sorted ascending")


class _VersionState:
    __slots__ = ("mode", "entered_at", "stage")

    def __init__(self) -> None:
        self.mode = CanaryMode.NORMAL
        self.entered_at = 0.0
        self.stage = 0


class CanaryController:
    """按 (路由, 版本) 维护灰度状态机，事件 + 时钟驱动，结果可复现。

    状态迁移只发生在两处：熔断 OPEN 事件（立即压零/退回）与
    effective_weight 求值时的惰性时间推进（恢复逐级放量）。
    CLOSED 事件被有意忽略：恢复进度只由时钟与「无新 OPEN」推进，
    迟到的成功结果不会把状态提前放开。
    """

    def __init__(self, config: CanaryConfig, clock: Clock | None = None) -> None:
        self._config = config
        self._clock: Clock = clock or SystemClock()
        self._lock = threading.Lock()
        self._states: dict[tuple[str, int], _VersionState] = {}
        self._versions_by_upstream: dict[str, list[tuple[str, int]]] = {}

    def register_version(
        self, route_id: str, version: int, upstream_names: tuple[str, ...]
    ) -> None:
        """登记版本与其上游集合的对应关系。"""
        with self._lock:
            self._states.setdefault((route_id, version), _VersionState())
            for name in upstream_names:
                self._versions_by_upstream.setdefault(name, []).append((route_id, version))

    def on_circuit_event(self, upstream_name: str, state: CircuitState) -> None:
        """熔断事件回调：OPEN -> 相关版本立即压零；CLOSED -> 有意忽略。"""
        if state is not CircuitState.OPEN:
            return
        now = self._clock.now()
        with self._lock:
            for key in self._versions_by_upstream.get(upstream_name, []):
                st = self._states[key]
                st.mode = CanaryMode.SUPPRESSED
                st.entered_at = now
                st.stage = 0
                log.info(
                    "canary suppress route=%s version=%d (upstream=%s open)",
                    key[0], key[1], upstream_name,
                )

    def effective_weight(self, route_id: str, version: int, base_weight: int) -> int:
        """按状态机求有效权重；按当前时钟惰性推进恢复阶段。"""
        if base_weight <= 0:
            return 0
        now = self._clock.now()
        with self._lock:
            st = self._states.setdefault((route_id, version), _VersionState())
            if st.mode is CanaryMode.NORMAL:
                return base_weight
            if st.mode is CanaryMode.SUPPRESSED:
                if now - st.entered_at < self._config.recovery_seconds:
                    return 0
                st.mode = CanaryMode.RECOVERING
                st.entered_at = now
                st.stage = 0
                log.info("canary recover route=%s version=%d stage=0", route_id, version)
                return self._ramp_weight(base_weight, 0)
            # RECOVERING：稳定经过 dwell 才升一级；期间再次 OPEN 已被事件退回
            while now - st.entered_at >= self._config.stage_dwell_seconds:
                st.stage += 1
                st.entered_at += self._config.stage_dwell_seconds
                if st.stage >= len(self._config.ramp):
                    st.mode = CanaryMode.NORMAL
                    log.info("canary restored route=%s version=%d", route_id, version)
                    return base_weight
                log.info(
                    "canary recover route=%s version=%d stage=%d", route_id, version, st.stage
                )
            return self._ramp_weight(base_weight, st.stage)

    def _ramp_weight(self, base_weight: int, stage: int) -> int:
        fraction = self._config.ramp[min(stage, len(self._config.ramp) - 1)]
        return max(1, round(base_weight * fraction))

    def mode(self, route_id: str, version: int) -> CanaryMode:
        with self._lock:
            return self._states.setdefault((route_id, version), _VersionState()).mode
