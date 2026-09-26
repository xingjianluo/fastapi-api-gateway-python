"""路由匹配与版本协商。"""

from __future__ import annotations

from gateway.errors import GatewayError
from gateway.models import Caller, GatewayRequest, Route, RouteVersion


class Router:
    """按方法+路径模板匹配路由，并按调用方可接受版本确定性选择版本。"""

    def __init__(self, routes: list[Route] | None = None) -> None:
        self._routes: list[Route] = list(routes or [])

    def add_route(self, route: Route) -> None:
        self._routes.append(route)

    def match(self, request: GatewayRequest) -> tuple[Route, dict[str, str]]:
        """匹配路由并提取路径参数；未命中抛 ROUTE_NOT_FOUND。"""
        raise NotImplementedError

    def select_version(self, route: Route, caller: Caller) -> RouteVersion:
        """在调用方可接受版本内确定性选择最高版本；无法满足抛 VERSION_NOT_SATISFIED。"""
        raise NotImplementedError
