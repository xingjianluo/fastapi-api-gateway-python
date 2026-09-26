"""路由、版本、上游与请求/响应的数据模型。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class Method(enum.Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"

    @property
    def idempotent(self) -> bool:
        return self in (Method.GET, Method.PUT, Method.DELETE)


@dataclass(frozen=True)
class Caller:
    """调用方身份。"""

    caller_id: str
    accepted_versions: frozenset[int] = field(default_factory=frozenset)


@dataclass(frozen=True)
class Upstream:
    """上游实例标识。"""

    name: str


@dataclass(frozen=True)
class RouteVersion:
    """逻辑接口的一个版本实现。"""

    version: int
    upstreams: tuple[Upstream, ...]


@dataclass(frozen=True)
class Route:
    """逻辑接口：方法 + 路径模板（可含 {param}）。"""

    route_id: str
    method: Method
    path_template: str
    versions: tuple[RouteVersion, ...]


@dataclass(frozen=True)
class GatewayRequest:
    caller: Caller
    method: Method
    path: str
    body: bytes = b""

    @property
    def idempotent(self) -> bool:
        return self.method.idempotent


@dataclass(frozen=True)
class GatewayResponse:
    status: int
    body: dict
    upstream: str
    version: int
