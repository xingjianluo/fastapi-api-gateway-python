"""上游熔断器：连续失败进入拒绝，半开探测受限放行。"""

from __future__ import annotations

import enum

from gateway.clock import Clock, SystemClock
from gateway.errors import GatewayError
from gateway.models import Upstream


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitConfig:
    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_seconds: float = 30.0,
        half_open_max_probes: int = 1,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self.half_open_max_probes = half_open_max_probes


class CircuitBreakerRegistry:
    """按上游维护熔断状态机。"""

    def __init__(self, config: CircuitConfig | None = None, clock: Clock | None = None) -> None:
        self._config = config or CircuitConfig()
        self._clock: Clock = clock or SystemClock()

    def before_request(self, upstream: Upstream) -> None:
        """请求前闸门；拒绝时抛 CIRCUIT_OPEN。"""
        raise NotImplementedError

    def on_success(self, upstream: Upstream) -> None:
        raise NotImplementedError

    def on_failure(self, upstream: Upstream) -> None:
        raise NotImplementedError

    def state(self, upstream: Upstream) -> CircuitState:
        raise NotImplementedError
