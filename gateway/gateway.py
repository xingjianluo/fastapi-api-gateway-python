"""网关编排：路由 -> 版本选择 -> 限流 -> 熔断 -> 容量 -> 上游调用 -> 故障切换。

版本选择：未配置权重表时保持原行为（可接受版本内取最高）；配置权重表后
按 (路由, 调用方) 哈希粘性分桶，权重可运行期整体替换；配置灰度联动控制器时，
有效权重为配置权重经健康保护调整后的值。

故障切换规则：
- 熔断拒绝（CIRCUIT_OPEN）与容量拒绝（UPSTREAM_CAPACITY_EXCEEDED）意味着
  请求未发出，任何请求都可安全尝试下一上游；
- 超时/不可用意味着请求可能已执行，仅幂等请求允许切换备用上游；
- 上游业务错误（4xx/5xx）说明上游已应答，不做切换，直接归一化返回；
- 容量占用与释放在 finally 中成对完成，在途请求绝不中途掐断；
- 所有底层异常在此归一化为 GatewayError，不向调用方透出。
"""

from __future__ import annotations

import logging

from gateway.canary import CanaryController
from gateway.capacity import UpstreamCapacityManager
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
from gateway.weights import VersionWeighter

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
        weighter: VersionWeighter | None = None,
        capacity: UpstreamCapacityManager | None = None,
        canary: CanaryController | None = None,
    ) -> None:
        self._router = router
        self._rate_limiter = rate_limiter
        self._circuits = circuits
        self._upstreams = upstreams
        self._timeout = timeout_seconds
        self._clock: Clock = clock or SystemClock()
        self._weighter = weighter
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
            acquired = False
            if self._capacity is not None:
                try:
                    self._capacity.acquire(request.caller.caller_id, upstream_ref.name)
                    acquired = True
                except GatewayError as exc:
                    # 容量拒绝意味着请求未发出，可安全尝试下一上游
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
                if acquired:
                    self._capacity.release(request.caller.caller_id, upstream_ref.name)
        assert last_error is not None
        raise last_error

    def _select_version(self, route: Route, caller: Caller) -> RouteVersion:
        """未启用权重时保持原协商逻辑；启用后按有效权重粘性选择。"""
        if self._weighter is None:
            return self._router.select_version(route, caller)
        acceptable = [
            v
            for v in route.versions
            if not caller.accepted_versions or v.version in caller.accepted_versions
        ]
        if not acceptable:
            raise GatewayError(
                ErrorCategory.VERSION_NOT_SATISFIED,
                f"caller '{caller.caller_id}' accepts {sorted(caller.accepted_versions)} "
                f"but route '{route.route_id}' offers "
                f"{sorted(v.version for v in route.versions)}",
            )
        configured = self._weighter.configured(route.route_id)
        if configured is None:
            # 该路由未配置权重表：保持原行为（最高可接受版本）
            return max(acceptable, key=lambda v: v.version)
        effective = (
            self._canary.effective_weights(route, configured)
            if self._canary is not None
            else dict(configured)
        )
        chosen = self._weighter.select(
            route.route_id, caller.caller_id, acceptable, effective
        )
        if chosen is None:
            raise GatewayError(
                ErrorCategory.VERSION_NOT_SATISFIED,
                f"all acceptable versions of route '{route.route_id}' have "
                f"effective weight 0 for caller '{caller.caller_id}'",
            )
        return chosen

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
