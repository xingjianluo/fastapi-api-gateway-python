"""网关编排：路由 -> 版本选择(加权+粘性) -> 限流 -> 容量 -> 熔断 -> 上游调用 -> 故障切换。

版本选择：
- 未配置 WeightManager 时保持旧行为（取可接受的最高版本）；
- 配置后按 (路由, 版本) 权重分流，同一调用方在权重不变时稳定落同一版本；
- 配置 CanaryController 时有效权重 = 基础权重经灰度状态机调整后的值；
- 可接受版本有效权重全为 0 时返回 upstream_unavailable（可重试，待灰度恢复）。

故障切换规则：
- 熔断拒绝（CIRCUIT_OPEN）与容量拒绝（UPSTREAM_CAPACITY_EXHAUSTED）意味着
  请求未发出，任何请求都可安全尝试下一上游；
- 超时/不可用意味着请求可能已执行，仅幂等请求允许切换备用上游；
- 上游业务错误（4xx/5xx）说明上游已应答，不做切换，直接归一化返回；
- 所有底层异常在此归一化为 GatewayError，不向调用方透出。
"""

from __future__ import annotations

import logging

from gateway.canary import CanaryController
from gateway.capacity import CapacityManager
from gateway.circuit import CircuitBreakerRegistry
from gateway.clock import Clock, SystemClock
from gateway.errors import ErrorCategory, GatewayError
from gateway.models import Caller, GatewayRequest, GatewayResponse, Route, RouteVersion
from gateway.ratelimit import RateLimiter
from gateway.router import Router
from gateway.upstreams import (
    MockUpstream,
    UpstreamBusinessFault,
    UpstreamTimeoutFault,
    UpstreamUnavailableFault,
)
from gateway.weights import WeightManager, pick_weighted

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
        weights: WeightManager | None = None,
        capacity: CapacityManager | None = None,
        canary: CanaryController | None = None,
    ) -> None:
        self._router = router
        self._rate_limiter = rate_limiter
        self._circuits = circuits
        self._upstreams = upstreams
        self._timeout = timeout_seconds
        self._clock: Clock = clock or SystemClock()
        self._weights = weights
        self._capacity = capacity
        self._canary = canary

    def handle(self, request: GatewayRequest) -> GatewayResponse:
        route, _params = self._router.match(request)
        version = self._select_version(route, request.caller)
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
            permit = None
            if self._capacity is not None:
                try:
                    permit = self._capacity.acquire(
                        upstream_ref.name, request.caller.caller_id
                    )
                except GatewayError as exc:
                    # 容量拒绝：请求未发出，可安全尝试下一上游
                    log.info("capacity rejected upstream=%s", upstream_ref.name)
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
            finally:
                if permit is not None:
                    permit.release()
        assert last_error is not None
        raise last_error

    def _select_version(self, route: Route, caller: Caller) -> RouteVersion:
        """未配权重时取最高可接受版本；否则按有效权重做调用方粘性选择。"""
        if self._weights is None:
            return self._router.select_version(route, caller)
        candidates = self._router.candidate_versions(route, caller)
        snapshot = self._weights.snapshot()
        weighted: list[tuple[int, int]] = []
        for v in candidates:
            base = snapshot.get((route.route_id, v.version), 1)
            effective = (
                self._canary.effective_weight(route.route_id, v.version, base)
                if self._canary is not None
                else base
            )
            if effective > 0:
                weighted.append((v.version, effective))
        if not weighted:
            raise GatewayError(
                ErrorCategory.UPSTREAM_UNAVAILABLE,
                f"all acceptable versions of route '{route.route_id}' "
                f"currently have weight 0",
                retryable=True,
            )
        chosen = pick_weighted(weighted, route.route_id, caller.caller_id)
        log.info(
            "weighted select route=%s caller=%s candidates=%s -> version=%d",
            route.route_id, caller.caller_id, weighted, chosen,
        )
        return next(v for v in candidates if v.version == chosen)

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
