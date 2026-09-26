"""网关编排：路由 -> 版本协商 -> 限流 -> 熔断 -> 上游调用 -> 故障切换。

故障切换规则：
- 熔断拒绝（CIRCUIT_OPEN）意味着请求未发出，任何请求都可尝试下一上游；
- 超时/不可用意味着请求可能已执行，仅幂等请求允许切换备用上游；
- 上游业务错误（4xx/5xx）说明上游已应答，不做切换，直接归一化返回；
- 所有底层异常在此归一化为 GatewayError，不向调用方透出。
"""

from __future__ import annotations

import logging

from gateway.circuit import CircuitBreakerRegistry
from gateway.clock import Clock, SystemClock
from gateway.errors import ErrorCategory, GatewayError
from gateway.models import GatewayRequest, GatewayResponse
from gateway.ratelimit import RateLimiter
from gateway.router import Router
from gateway.upstreams import (
    MockUpstream,
    UpstreamBusinessFault,
    UpstreamTimeoutFault,
    UpstreamUnavailableFault,
)

log = logging.getLogger(__name__)


class Gateway:
    def __init__(
        self,
        router: Router,
        rate_limiter: RateLimiter,
        circuits: CircuitBreakerRegistry,
        upstreams: dict[str, MockUpstream],
        *,
        timeout_seconds: float = 2.0,
        clock: Clock | None = None,
    ) -> None:
        self._router = router
        self._rate_limiter = rate_limiter
        self._circuits = circuits
        self._upstreams = upstreams
        self._timeout = timeout_seconds
        self._clock: Clock = clock or SystemClock()

    def handle(self, request: GatewayRequest) -> GatewayResponse:
        route, _params = self._router.match(request)
        version = self._router.select_version(route, request.caller)
        self._rate_limiter.check(request.caller.caller_id, route.route_id)
        log.info(
            "route=%s version=%d caller=%s idempotent=%s",
            route.route_id, version.version, request.caller.caller_id, request.idempotent,
        )

        last_error: GatewayError | None = None
        for upstream_ref in version.upstreams:
            mock = self._upstreams[upstream_ref.name]
            try:
                self._circuits.before_request(upstream_ref)
            except GatewayError as exc:
                # 请求未发出，可安全尝试下一上游
                log.info("circuit rejected upstream=%s", upstream_ref.name)
                last_error = exc
                continue
            try:
                return self._call(mock, upstream_ref.name, request)
            except GatewayError as exc:
                last_error = exc
                if exc.category in (
                    ErrorCategory.UPSTREAM_TIMEOUT,
                    ErrorCategory.UPSTREAM_UNAVAILABLE,
                ) and not request.idempotent:
                    # 请求可能已执行，非幂等请求禁止重试/切换
                    log.info("non-idempotent request, no failover: %s", exc.category)
                    raise
                if exc.category is ErrorCategory.UPSTREAM_ERROR:
                    # 上游已应答业务错误，不切换
                    raise
                log.info("failover from upstream=%s reason=%s", upstream_ref.name, exc.category)
                continue
        assert last_error is not None
        raise last_error

    def _call(self, mock: MockUpstream, name: str, request: GatewayRequest) -> GatewayResponse:
        start = self._clock.now()
        try:
            response = mock.call(request, self._timeout)
        except UpstreamTimeoutFault as exc:
            self._circuits.on_failure(mock.upstream)
            raise GatewayError(
                ErrorCategory.UPSTREAM_TIMEOUT, str(exc), retryable=True
            ) from exc
        except UpstreamUnavailableFault as exc:
            self._circuits.on_failure(mock.upstream)
            raise GatewayError(
                ErrorCategory.UPSTREAM_UNAVAILABLE, str(exc), retryable=True
            ) from exc
        except UpstreamBusinessFault as exc:
            # 5xx 记为熔断失败，4xx 视为上游健康（调用方问题）
            if exc.status >= 500:
                self._circuits.on_failure(mock.upstream)
            else:
                self._circuits.on_success(mock.upstream)
            raise GatewayError(
                ErrorCategory.UPSTREAM_ERROR,
                f"upstream '{name}' returned business error status={exc.status}",
                retryable=False,
            ) from exc
        except GatewayError:
            raise
        except Exception as exc:  # 兜底：底层异常一律归一化
            self._circuits.on_failure(mock.upstream)
            raise GatewayError(
                ErrorCategory.UPSTREAM_ERROR,
                f"upstream '{name}' failed: {type(exc).__name__}",
                retryable=True,
            ) from exc
        elapsed = self._clock.now() - start
        if elapsed > self._timeout:
            self._circuits.on_failure(mock.upstream)
            raise GatewayError(
                ErrorCategory.UPSTREAM_TIMEOUT,
                f"upstream '{name}' exceeded timeout {self._timeout}s (took {elapsed:.3f}s)",
                retryable=True,
            )
        self._circuits.on_success(mock.upstream)
        return response
