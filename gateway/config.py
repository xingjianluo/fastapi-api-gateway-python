"""Configuration model: routes, versions, rate-limit rules and breaker tuning."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class UpstreamConfig:
    name: str
    kind: str = "ok"                 # simulated behaviour: ok|fail|timeout|busy|script
    delay: float = 0.0
    status: int = 200
    body: object = None
    sequence: tuple[str, ...] = ()   # for kind="script": per-call outcomes


@dataclass(frozen=True)
class RateLimitRule:
    # Token bucket (burst control)
    capacity: float = 100.0          # max burst; request cost > capacity -> scope_insufficient
    refill_rate: float = 10.0        # tokens per second
    # Long-term quota (refilled per quota_window seconds)
    quota_limit: int | None = None
    quota_window: float = 60.0


@dataclass(frozen=True)
class Route:
    path: str                        # e.g. "/orders/{order_id}"
    methods: frozenset[str]
    versions: tuple[str, ...]        # available versions, e.g. ("1.0.0", "2.0.0")
    upstreams: tuple[UpstreamConfig, ...]
    rate_limit: RateLimitRule | None = None


@dataclass(frozen=True)
class CircuitBreakerConfig:
    failure_threshold: int = 5
    cooldown_seconds: float = 30.0
    half_open_max_probe: int = 1     # limited traffic allowed while probing
    probe_success_threshold: int = 1


@dataclass(frozen=True)
class GatewayConfig:
    routes: tuple[Route, ...]
    default_upstreams: tuple[UpstreamConfig, ...] = ()
    circuit_breaker: CircuitBreakerConfig = field(
        default_factory=CircuitBreakerConfig
    )
