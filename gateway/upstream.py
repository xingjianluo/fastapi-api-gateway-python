"""Simulated upstream services (no real network needed to run locally).

Each upstream is an in-process object whose behaviour comes from
:class:`UpstreamConfig`:

* ``ok``      -> succeeds with ``status`` / ``body``, declaring ``delay`` seconds
* ``busy``    -> business error (HTTP 5xx); gateway maps it to
                 ``upstream_business_error`` and counts it as breaker failure
* ``fail``    -> connection-style failure -> ``upstream_unavailable``
* ``timeout`` -> declares work longer than the remaining budget ->
                 ``upstream_timeout``
* ``script``  -> outcomes taken in order from ``sequence`` (last one repeats)

Timing is **declarative**, not blocking: an upstream reports how long its work
would take, and when that exceeds the remaining time budget the gateway treats
it as a timeout.  No threads or wall-clock waits are involved, so every
timeout test is instant and reproducible under :class:`FakeClock`.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .clock import Clock, FakeClock
from .config import UpstreamConfig
from .errors import (
    UpstreamBusinessError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)


@dataclass(frozen=True)
class UpstreamRequest:
    method: str
    path: str
    path_params: dict[str, str]
    version: str
    caller: str
    body: bytes
    attempt: int                       # 0 = primary, 1+ = failover
    remaining_budget: float            # seconds left for this whole call chain
    timeout: float                     # configured per-attempt timeout


@dataclass(frozen=True)
class UpstreamResponse:
    status: int
    body: object
    upstream_name: str
    version: str
    retried: bool
    delay: float = 0.0                 # simulated latency consumed


class SimulatedUpstream:
    """Deterministic in-process upstream; behaviour selected from config."""

    def __init__(self, config: UpstreamConfig, clock: Clock | None = None) -> None:
        self._config = config
        self._clock = clock
        self._fake = isinstance(clock, FakeClock)
        self._lock = threading.Lock()
        self._call_index = 0

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def delay(self) -> float:
        return self._config.delay

    @property
    def call_count(self) -> int:
        with self._lock:
            return self._call_index

    def reset(self) -> None:
        with self._lock:
            self._call_index = 0

    def call(self, request: UpstreamRequest) -> UpstreamResponse:
        outcome = self._next_outcome()

        # Only successful work (or an explicitly slow upstream) consumes the
        # latency budget; connection failures and business errors surface
        # immediately.
        if outcome == "timeout":
            required_delay = max(self.delay, request.timeout + 0.001)
        elif outcome == "ok":
            required_delay = self.delay
        else:
            required_delay = 0.0

        if required_delay > 0:
            if self._fake:
                # Declarative mode: the clock never moves on its own, so
                # compare the declared work duration against the budget and
                # fail instantly — keeps tests fast and reproducible.
                if required_delay > request.remaining_budget + 1e-9:
                    raise UpstreamTimeoutError(
                        f"upstream {self.name!r} would take {required_delay:g}s "
                        f"but only {request.remaining_budget:g}s budget remains",
                        details={
                            "upstream": self.name,
                            "attempt": request.attempt,
                            "required_delay": required_delay,
                            "remaining_budget": round(
                                request.remaining_budget, 6),
                        },
                    )
            else:
                # Real-clock mode: actually wait, but never past the budget.
                assert self._clock is not None
                wait = min(required_delay, request.remaining_budget)
                self._clock.sleep(wait)
                if required_delay > request.remaining_budget + 1e-9:
                    raise UpstreamTimeoutError(
                        f"upstream {self.name!r} timed out after "
                        f"{request.timeout:g}s",
                        details={
                            "upstream": self.name,
                            "attempt": request.attempt,
                            "timeout": request.timeout,
                        },
                    )

        if outcome == "ok":
            body = self._config.body
            if body is None:
                body = {
                    "served_by": self.name,
                    "path": request.path,
                    "params": request.path_params,
                    "version": request.version,
                    "attempt": request.attempt,
                }
            return UpstreamResponse(
                status=self._config.status,
                body=body,
                upstream_name=self.name,
                version=request.version,
                retried=request.attempt > 0,
                delay=required_delay,
            )

        if outcome == "busy":
            raise UpstreamBusinessError(
                f"upstream {self.name!r} returned business error "
                f"status {self._config.status}",
                details={
                    "upstream": self.name,
                    "status": self._config.status,
                    "attempt": request.attempt,
                },
            )

        # outcome == "fail": connection refused / reset style failure.
        raise UpstreamUnavailableError(
            f"upstream {self.name!r} unavailable (simulated connection failure)",
            details={"upstream": self.name, "attempt": request.attempt},
        )

    def _next_outcome(self) -> str:
        with self._lock:
            seq = self._config.sequence
            if self._config.kind == "script" and seq:
                outcome = seq[min(self._call_index, len(seq) - 1)]
            else:
                outcome = self._config.kind
            self._call_index += 1
            return outcome


class UpstreamPool:
    """Builds and caches one :class:`SimulatedUpstream` per name."""

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock = clock
        self._pool: dict[str, SimulatedUpstream] = {}
        self._lock = threading.Lock()

    def get(self, config: UpstreamConfig) -> SimulatedUpstream:
        with self._lock:
            upstream = self._pool.get(config.name)
            if upstream is None:
                upstream = SimulatedUpstream(config, self._clock)
                self._pool[config.name] = upstream
            return upstream

    def reset(self) -> None:
        with self._lock:
            for upstream in self._pool.values():
                upstream.reset()
