"""Gateway facade: orchestrates routing, rate limiting, breaker and failover.

Request pipeline (deterministic decision order):

1. parse the accepted-version header;
2. route (method + path) and negotiate the concrete version;
3. apply rate limit / quota for the (caller, route) pair;
4. try upstreams in configured order, consulting the circuit breaker before
   each call.  Only idempotent methods are retried on a different upstream and
   only after *unavailability / timeout / open-circuit* — a business response
   (even HTTP 5xx) is a definitive answer and is never replayed; non-idempotent
   requests are never executed twice;
5. normalise every failure to one distinct :class:`~gateway.errors.ErrorCode`.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

from .breaker import BreakerState, CircuitBreakerRegistry
from .clock import Clock, SystemClock
from .config import GatewayConfig
from .errors import (
    CircuitOpenError,
    GatewayError,
    UpstreamError,
)
from .ratelimiter import RateLimiter
from .router import IDEMPOTENT_METHODS, Router
from .upstream import UpstreamPool, UpstreamRequest, UpstreamResponse
from .versioning import VersionRange

logger = logging.getLogger("gateway")

# Failover-worthy conditions: transport problems where the upstream's
# disposition of the request is unknown.  Business responses are NOT included.
RETRYABLE_CODES = frozenset(
    {
        "upstream_timeout",
        "upstream_unavailable",
        "upstream_circuit_open",
    }
)


@dataclass
class GatewayRequest:
    method: str
    path: str
    caller: str = "anonymous"
    version_header: str | None = None
    body: bytes = b""
    timeout: float = 5.0
    cost: float = 1.0


@dataclass
class GatewayResponse:
    status: int
    body: object
    upstream: str | None
    version: str | None
    retried: bool = False
    error_code: str | None = None


class Gateway:
    """Thread-safe request pipeline."""

    def __init__(self, config: GatewayConfig, clock: Clock | None = None) -> None:
        self._clock = clock or SystemClock()
        self._lock = threading.RLock()
        self._config = config
        self._router = Router(list(config.routes))
        self._rate_limiter = RateLimiter(self._clock)
        self._breaker = CircuitBreakerRegistry(config.circuit_breaker, self._clock)
        self._pool = UpstreamPool(self._clock)
        self._rate_limiter.update_rules(self._build_rule_table(config))

    @staticmethod
    def _build_rule_table(config: GatewayConfig) -> dict[str, object]:
        return {route.path: route.rate_limit for route in config.routes}

    @property
    def breaker(self) -> CircuitBreakerRegistry:
        return self._breaker

    @property
    def pool(self) -> UpstreamPool:
        return self._pool

    def update_config(self, config: GatewayConfig) -> None:
        """Atomically swap router + rate-limit rules (breaker state preserved)."""
        with self._lock:
            router = Router(list(config.routes))
            rules = self._build_rule_table(config)
            # Swap both references inside the same lock-held section; every
            # in-flight request holds references from before this point.
            self._config = config
            self._router = router
            self._rate_limiter.update_rules(rules)
            logger.info("gateway config atomically updated: %d routes",
                        len(config.routes))

    def handle(self, request: GatewayRequest) -> GatewayResponse:
        """Run the full pipeline; never raises: errors become GatewayResponse."""
        method = request.method.upper()
        try:
            requested = (
                VersionRange.parse(request.version_header)
                if request.version_header
                else None
            )
            # Snapshot router + config atomically so the whole request is
            # decided against exactly one configuration generation.
            with self._lock:
                router = self._router
                config = self._config
            match = router.resolve(method, request.path, requested)
            logger.info(
                "route resolved | caller=%s method=%s path=%s version_header=%r "
                "-> route=%s version=%s params=%s",
                request.caller, method, request.path, request.version_header,
                match.route.path, match.version_str, match.path_params,
            )

            decision = self._rate_limiter.check(
                caller=request.caller,
                route_key=match.route.path,
                rule=match.route.rate_limit,
                cost=request.cost,
            )
            logger.info(
                "rate limit | caller=%s route=%s cost=%g allowed=True "
                "tokens_left=%.3f quota_left=%s",
                request.caller, match.route.path, request.cost,
                decision.remaining_tokens, decision.remaining_quota,
            )

            return self._dispatch_with_failover(request, match, config)
        except GatewayError as exc:
            logger.warning(
                "request rejected | caller=%s method=%s path=%s reason=%s details=%s",
                request.caller, method, request.path, exc.code.value, exc.details,
            )
            return GatewayResponse(
                status=exc.http_status,
                body=exc.to_dict(),
                upstream=None,
                version=None,
                error_code=exc.code.value,
            )
        except Exception:  # pragma: no cover - defensive normalisation
            logger.exception("unexpected internal error")
            return GatewayResponse(
                status=500,
                body={"error": {"code": "internal_error",
                                "message": "internal gateway error",
                                "details": {}}},
                upstream=None,
                version=None,
                error_code="internal_error",
            )

    def _dispatch_with_failover(self, request: GatewayRequest, match,
                                config: GatewayConfig) -> GatewayResponse:
        idempotent = request.method.upper() in IDEMPOTENT_METHODS
        upstreams = list(match.route.upstreams) or list(config.default_upstreams)
        # Each attempt gets its own per-attempt timeout budget.  Whether
        # another attempt is *allowed* depends solely on idempotence and the
        # failure category (see RETRYABLE_CODES).
        last_error: GatewayError | None = None

        for index, upstream_config in enumerate(upstreams):
            is_retry = index > 0
            if is_retry and not idempotent:
                # Non-idempotent: never execute a second time.
                logger.info(
                    "failover suppressed (non-idempotent) | method=%s route=%s "
                    "primary_error=%s",
                    request.method, match.route.path,
                    last_error.code.value if last_error else None,
                )
                break

            upstream = self._pool.get(upstream_config)
            try:
                snapshot = self._breaker.allow_request(upstream.name)
                logger.info(
                    "breaker allow | upstream=%s state=%s failures=%d",
                    upstream.name, snapshot.state.value,
                    snapshot.consecutive_failures,
                )
            except CircuitOpenError as exc:
                last_error = exc
                logger.info(
                    "breaker reject | upstream=%s retry_after=%.3f",
                    upstream.name, exc.details.get("retry_after_seconds", 0.0),
                )
                continue

            upstream_request = UpstreamRequest(
                method=request.method,
                path=request.path,
                path_params=match.path_params,
                version=match.version_str,
                caller=request.caller,
                body=request.body,
                attempt=index,
                remaining_budget=request.timeout,
                timeout=request.timeout,
            )
            try:
                response = upstream.call(upstream_request)
            except GatewayError as exc:
                self._breaker.record_failure(upstream.name)
                last_error = exc
                logger.info(
                    "upstream failure | upstream=%s attempt=%d idempotent=%s "
                    "code=%s retryable=%s",
                    upstream.name, index, idempotent, exc.code.value,
                    exc.code.value in RETRYABLE_CODES,
                )
                if exc.code.value not in RETRYABLE_CODES or not idempotent or index == len(upstreams) - 1:
                    break
                continue

            self._breaker.record_success(upstream.name)
            logger.info(
                "upstream success | upstream=%s attempt=%d status=%d delay=%.3f",
                upstream.name, index, response.status, response.delay,
            )
            return GatewayResponse(
                status=response.status,
                body=response.body,
                upstream=upstream.name,
                version=match.version_str,
                retried=response.retried,
            )

        assert last_error is not None
        return GatewayResponse(
            status=last_error.http_status,
            body=last_error.to_dict(),
            upstream=None,
            version=match.version_str,
            retried=False,
            error_code=last_error.code.value,
        )
