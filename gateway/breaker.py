"""Per-upstream circuit breaker: closed -> open -> half-open -> closed/open.

State machine (all transitions happen under one lock, so concurrent probes
observe a single stable decision):

* ``CLOSED``    : normal traffic. ``failure_threshold`` consecutive failures
                  trip the breaker to ``OPEN``.
* ``OPEN``      : all traffic rejected with ``upstream_circuit_open``.  After
                  ``cooldown_seconds`` the first request moves the breaker to
                  ``HALF_OPEN`` and becomes a probe.
* ``HALF_OPEN``  : at most ``half_open_max_probe`` in-flight requests are
                  admitted as probes; the rest are still rejected.  Any probe
                  failure immediately re-opens (stable failure rule).
                  ``probe_success_threshold`` successful probes close again.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import Enum

from .clock import Clock
from .config import CircuitBreakerConfig
from .errors import CircuitOpenError


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True)
class BreakerSnapshot:
    state: BreakerState
    upstream: str
    consecutive_failures: int
    probe_successes: int
    probes_in_flight: int


@dataclass
class _Breaker:
    state: BreakerState = BreakerState.CLOSED
    consecutive_failures: int = 0
    opened_at: float = 0.0
    probe_successes: int = 0
    probes_in_flight: int = 0


class CircuitBreakerRegistry:
    """Holds one breaker per upstream name; transitions are fully synchronised."""

    def __init__(self, config: CircuitBreakerConfig, clock: Clock) -> None:
        self._config = config
        self._clock = clock
        self._breakers: dict[str, _Breaker] = {}
        self._lock = threading.RLock()

    def _get(self, upstream: str) -> _Breaker:
        breaker = self._breakers.get(upstream)
        if breaker is None:
            breaker = _Breaker()
            self._breakers[upstream] = breaker
        return breaker

    def allow_request(self, upstream: str) -> BreakerSnapshot:
        """Admit traffic / a probe, or raise :class:`CircuitOpenError`."""
        with self._lock:
            breaker = self._get(upstream)
            now = self._clock.now()

            if breaker.state is BreakerState.OPEN:
                if now < breaker.opened_at + self._config.cooldown_seconds:
                    raise CircuitOpenError(
                        f"upstream {upstream!r} circuit open",
                        details=self._details(upstream, breaker, now),
                    )
                # Cooldown elapsed -> enter probing phase deterministically.
                breaker.state = BreakerState.HALF_OPEN
                breaker.probe_successes = 0
                breaker.probes_in_flight = 0

            if breaker.state is BreakerState.HALF_OPEN:
                if breaker.probes_in_flight >= self._config.half_open_max_probe:
                    raise CircuitOpenError(
                        f"upstream {upstream!r} probing in progress",
                        details=self._details(upstream, breaker, now),
                    )
                breaker.probes_in_flight += 1

            return self._snapshot(upstream, breaker)

    def record_success(self, upstream: str) -> BreakerSnapshot:
        with self._lock:
            breaker = self._get(upstream)
            if breaker.state is BreakerState.HALF_OPEN:
                breaker.probes_in_flight = max(0, breaker.probes_in_flight - 1)
                breaker.probe_successes += 1
                if breaker.probe_successes >= self._config.probe_success_threshold:
                    breaker.state = BreakerState.CLOSED
                    breaker.consecutive_failures = 0
                    breaker.opened_at = 0.0
            elif breaker.state is BreakerState.CLOSED:
                breaker.consecutive_failures = 0
            return self._snapshot(upstream, breaker)

    def record_failure(self, upstream: str) -> BreakerSnapshot:
        with self._lock:
            breaker = self._get(upstream)
            now = self._clock.now()
            if breaker.state is BreakerState.HALF_OPEN:
                # A single failed probe re-opens: failure judgement is stable.
                breaker.probes_in_flight = max(0, breaker.probes_in_flight - 1)
                breaker.probe_successes = 0
                breaker.state = BreakerState.OPEN
                breaker.opened_at = now
            elif breaker.state is BreakerState.CLOSED:
                breaker.consecutive_failures += 1
                if breaker.consecutive_failures >= self._config.failure_threshold:
                    breaker.state = BreakerState.OPEN
                    breaker.opened_at = now
            return self._snapshot(upstream, breaker)

    def snapshot(self, upstream: str) -> BreakerSnapshot:
        with self._lock:
            return self._snapshot(upstream, self._get(upstream))

    def _details(self, upstream: str, breaker: _Breaker, now: float) -> dict:
        retry_after = max(
            0.0, breaker.opened_at + self._config.cooldown_seconds - now
        )
        return {
            "upstream": upstream,
            "state": breaker.state.value,
            "retry_after_seconds": round(retry_after, 6),
            "probes_in_flight": breaker.probes_in_flight,
        }

    @staticmethod
    def _snapshot(upstream: str, breaker: _Breaker) -> BreakerSnapshot:
        return BreakerSnapshot(
            state=breaker.state,
            upstream=upstream,
            consecutive_failures=breaker.consecutive_failures,
            probe_successes=breaker.probe_successes,
            probes_in_flight=breaker.probes_in_flight,
        )
