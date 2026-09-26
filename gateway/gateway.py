"""网关编排：路由 -> 限流 -> 熔断 -> 上游调用 -> 故障切换。"""

from __future__ import annotations

from gateway.circuit import CircuitBreakerRegistry
from gateway.errors import GatewayError
from gateway.models import GatewayRequest, GatewayResponse
from gateway.ratelimit import RateLimiter
from gateway.router import Router
from gateway.upstreams import MockUpstream


class Gateway:
    def __init__(
        self,
        router: Router,
        rate_limiter: RateLimiter,
        circuits: CircuitBreakerRegistry,
        upstreams: dict[str, MockUpstream],
        *,
        timeout_seconds: float = 2.0,
    ) -> None:
        self._router = router
        self._rate_limiter = rate_limiter
        self._circuits = circuits
        self._upstreams = upstreams
        self._timeout = timeout_seconds

    def handle(self, request: GatewayRequest) -> GatewayResponse:
        raise NotImplementedError
