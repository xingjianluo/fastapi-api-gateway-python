"""路由匹配与版本协商。"""

from __future__ import annotations

import re

from gateway.errors import ErrorCategory, GatewayError
from gateway.models import Caller, GatewayRequest, Route, RouteVersion

_PARAM_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


def _compile_template(template: str) -> re.Pattern[str]:
    """把 /items/{item_id} 编译为正则，命名组提取路径参数。"""
    parts: list[str] = []
    pos = 0
    for m in _PARAM_RE.finditer(template):
        parts.append(re.escape(template[pos : m.start()]))
        parts.append(f"(?P<{m.group(1)}>[^/]+)")
        pos = m.end()
    parts.append(re.escape(template[pos:]))
    return re.compile("^" + "".join(parts) + "$")


class Router:
    """按方法+路径模板匹配路由，并按调用方可接受版本确定性选择版本。

    匹配规则：方法必须一致；路径按注册顺序首个命中者生效（注册方需避免歧义）。
    版本协商：在调用方 accepted_versions 与路由提供版本的交集中取最高版本；
    accepted_versions 为空表示接受任意版本。
    """

    def __init__(self, routes: list[Route] | None = None) -> None:
        self._routes: list[Route] = []
        self._compiled: dict[str, re.Pattern[str]] = {}
        for route in routes or []:
            self.add_route(route)

    def add_route(self, route: Route) -> None:
        self._routes.append(route)
        self._compiled[route.route_id] = _compile_template(route.path_template)

    def match(self, request: GatewayRequest) -> tuple[Route, dict[str, str]]:
        """匹配路由并提取路径参数；未命中抛 ROUTE_NOT_FOUND。"""
        for route in self._routes:
            if route.method is not request.method:
                continue
            m = self._compiled[route.route_id].match(request.path)
            if m is not None:
                return route, m.groupdict()
        raise GatewayError(
            ErrorCategory.ROUTE_NOT_FOUND,
            f"no route for {request.method.value} {request.path}",
        )

    def select_version(self, route: Route, caller: Caller) -> RouteVersion:
        """在调用方可接受版本内确定性选择最高版本；无法满足抛 VERSION_NOT_SATISFIED。"""
        offered = sorted(v.version for v in route.versions)
        acceptable = (
            [v for v in route.versions if v.version in caller.accepted_versions]
            if caller.accepted_versions
            else list(route.versions)
        )
        if not acceptable:
            raise GatewayError(
                ErrorCategory.VERSION_NOT_SATISFIED,
                f"caller '{caller.caller_id}' accepts {sorted(caller.accepted_versions)} "
                f"but route '{route.route_id}' offers {offered}",
            )
        return max(acceptable, key=lambda v: v.version)
