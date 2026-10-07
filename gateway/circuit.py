"""上游熔断器：连续失败进入拒绝，半开探测受限放行。"""

from __future__ import annotations

import enum
import threading
from collections.abc import Callable

from gateway.clock import Clock, SystemClock
from gateway.errors import ErrorCategory, GatewayError
from gateway.models import Upstream


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitConfig:
    """熔断参数。

    failure_threshold: 连续失败达到该次数后熔断。
    recovery_seconds: 熔断后经过该时长才允许进入半开探测。
    half_open_max_probes: 半开期间允许同时通过的探测请求上限。
    """

    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_seconds: float = 30.0,
        half_open_max_probes: int = 1,
    ) -> None:
        if failure_threshold < 1 or recovery_seconds <= 0 or half_open_max_probes < 1:
            raise ValueError("invalid circuit config")
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self.half_open_max_probes = half_open_max_probes


class _Circuit:
    __slots__ = ("state", "consecutive_failures", "opened_at", "in_flight_probes")

    def __init__(self) -> None:
        self.state = CircuitState.CLOSED
        self.consecutive_failures = 0
        self.opened_at = 0.0
        self.in_flight_probes = 0


class CircuitBreakerRegistry:
    """按上游维护熔断状态机，线程安全，时间由注入时钟驱动。"""

    def __init__(self, config: CircuitConfig | None = None, clock: Clock | None = None) -> None:
        self._config = config or CircuitConfig()
        self._clock: Clock = clock or SystemClock()
        self._lock = threading.Lock()
        self._circuits: dict[str, _Circuit] = {}
        self._listeners: list[Callable[[str, CircuitState], None]] = []

    def add_listener(self, listener: Callable[[str, CircuitState], None]) -> None:
        """注册状态变更监听：进入 OPEN 或 CLOSED 时回调（上游名, 新状态）。

        回调在注册表锁外触发，监听器可以安全地回调注册表。
        """
        with self._lock:
            self._listeners.append(listener)

    def _notify(self, upstream: Upstream, state: CircuitState) -> None:
        for listener in self._listeners:
            listener(upstream.name, state)

    def _circuit_for(self, upstream: Upstream) -> _Circuit:
        return self._circuits.setdefault(upstream.name, _Circuit())

    def before_request(self, upstream: Upstream) -> None:
        """请求前闸门；熔断拒绝时抛 CIRCUIT_OPEN。半开期间占用一个探测名额。"""
        with self._lock:
            circuit = self._circuit_for(upstream)
            if circuit.state is CircuitState.CLOSED:
                return
            if circuit.state is CircuitState.OPEN:
                elapsed = self._clock.now() - circuit.opened_at
                if elapsed < self._config.recovery_seconds:
                    raise GatewayError(
                        ErrorCategory.CIRCUIT_OPEN,
                        f"circuit open for upstream '{upstream.name}' "
                        f"(retry after {self._config.recovery_seconds - elapsed:.3f}s)",
                        retryable=True,
                    )
                circuit.state = CircuitState.HALF_OPEN
                circuit.in_flight_probes = 0
            # HALF_OPEN：仅放行受限数量的探测
            if circuit.in_flight_probes >= self._config.half_open_max_probes:
                raise GatewayError(
                    ErrorCategory.CIRCUIT_OPEN,
                    f"circuit half-open for upstream '{upstream.name}', probe slots exhausted",
                    retryable=True,
                )
            circuit.in_flight_probes += 1

    def on_success(self, upstream: Upstream) -> None:
        """成功：关闭熔断并清零失败计数（半开探测成功即恢复）。"""
        with self._lock:
            circuit = self._circuit_for(upstream)
            was = circuit.state
            circuit.state = CircuitState.CLOSED
            circuit.consecutive_failures = 0
            circuit.in_flight_probes = 0
        if was is not CircuitState.CLOSED:
            self._notify(upstream, CircuitState.CLOSED)

    def on_failure(self, upstream: Upstream) -> None:
        """失败：CLOSED 累计连续失败；HALF_OPEN 探测失败立即重新熔断。"""
        opened = False
        with self._lock:
            circuit = self._circuit_for(upstream)
            if circuit.state is CircuitState.HALF_OPEN:
                circuit.state = CircuitState.OPEN
                circuit.opened_at = self._clock.now()
                circuit.in_flight_probes = 0
                circuit.consecutive_failures = self._config.failure_threshold
                opened = True
            else:
                circuit.consecutive_failures += 1
                if circuit.consecutive_failures >= self._config.failure_threshold:
                    circuit.state = CircuitState.OPEN
                    circuit.opened_at = self._clock.now()
                    opened = True
        if opened:
            self._notify(upstream, CircuitState.OPEN)

    def state(self, upstream: Upstream) -> CircuitState:
        with self._lock:
            return self._circuit_for(upstream).state
