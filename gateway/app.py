"""FastAPI 暴露层：把 HTTP 请求翻译为网关请求，错误类别映射为 HTTP 状态码。

本地运行：uv run python main.py
调用方通过请求头声明身份与可接受版本：
  X-Caller-Id: 调用方标识（缺省 anonymous）
  X-Accept-Versions: 逗号分隔的版本号（缺省接受任意版本）
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from gateway.circuit import CircuitBreakerRegistry, CircuitConfig
from gateway.errors import ErrorCategory, GatewayError
from gateway.gateway import Gateway
from gateway.models import Caller, GatewayRequest, Method, Route, RouteVersion, Upstream
from gateway.ratelimit import RateLimiter, RateLimitRule
from gateway.router import Router
from gateway.upstreams import Behavior, MockUpstream

_STATUS_BY_CATEGORY = {
    ErrorCategory.ROUTE_NOT_FOUND: 404,
    ErrorCategory.VERSION_NOT_SATISFIED: 406,
    ErrorCategory.RATE_LIMITED: 429,
    ErrorCategory.QUOTA_EXHAUSTED: 429,
    ErrorCategory.CIRCUIT_OPEN: 503,
    ErrorCategory.UPSTREAM_TIMEOUT: 504,
    ErrorCategory.UPSTREAM_UNAVAILABLE: 503,
    ErrorCategory.UPSTREAM_ERROR: 502,
}


def build_gateway() -> Gateway:
    """装配本地演示用网关：两条路由、两个版本的模拟上游、限流与熔断。"""
    items_v1 = Upstream("items-v1")
    items_v2a = Upstream("items-v2a")
    items_v2b = Upstream("items-v2b")
    router = Router(
        [
            Route(
                "get-item",
                Method.GET,
                "/items/{item_id}",
                (
                    RouteVersion(1, (items_v1,)),
                    RouteVersion(2, (items_v2a, items_v2b)),
                ),
            ),
            Route(
                "create-item",
                Method.POST,
                "/items",
                (RouteVersion(2, (items_v2a, items_v2b)),),
            ),
        ]
    )
    rate_limiter = RateLimiter()
    rate_limiter.update_rules(
        {
            ("anonymous", "get-item"): RateLimitRule(
                burst_limit=100, window_seconds=1.0, quota=100_000
            ),
        }
    )
    upstreams = {
        "items-v1": MockUpstream(items_v1, [Behavior("ok", body={"svc": "v1"})], version=1),
        "items-v2a": MockUpstream(items_v2a, [Behavior("ok", body={"svc": "v2a"})], version=2),
        "items-v2b": MockUpstream(items_v2b, [Behavior("ok", body={"svc": "v2b"})], version=2),
    }
    return Gateway(
        router,
        rate_limiter,
        CircuitBreakerRegistry(CircuitConfig(failure_threshold=3, recovery_seconds=30.0)),
        upstreams,
        timeout_seconds=2.0,
    )


def create_app(gateway: Gateway | None = None) -> FastAPI:
    gw = gateway or build_gateway()
    app = FastAPI(title="local-api-gateway")
    app.state.gateway = gw

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_req: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(
            status_code=_STATUS_BY_CATEGORY[exc.category], content=exc.to_dict()
        )

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
    async def proxy(path: str, request: Request) -> JSONResponse:
        caller = Caller(
            caller_id=request.headers.get("x-caller-id", "anonymous"),
            accepted_versions=frozenset(
                int(v) for v in request.headers.get("x-accept-versions", "").split(",") if v.strip()
            ),
        )
        gw_request = GatewayRequest(
            caller=caller,
            method=Method(request.method),
            path="/" + path,
            body=await request.body(),
        )
        response = gw.handle(gw_request)
        return JSONResponse(
            status_code=response.status,
            content={
                "upstream": response.upstream,
                "version": response.version,
                "body": response.body,
            },
        )

    return app
