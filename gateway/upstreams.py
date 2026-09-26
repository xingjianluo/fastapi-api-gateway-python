"""仓库内模拟上游服务，可脚本化延迟、失败与业务错误。

模拟上游抛出的内部故障仅用于网关内部归一化，不得透出给调用方。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Literal

from gateway.clock import Clock, SystemClock
from gateway.models import GatewayRequest, GatewayResponse, Upstream


class UpstreamFault(Exception):
    """模拟上游内部故障基类（网关内部使用）。"""


class UpstreamTimeoutFault(UpstreamFault):
    """上游在超时预算内未完成。"""


class UpstreamUnavailableFault(UpstreamFault):
    """上游连接失败/不可用。"""


class UpstreamBusinessFault(UpstreamFault):
    """上游返回了业务错误（HTTP 5xx/4xx）。"""

    def __init__(self, status: int, body: dict) -> None:
        super().__init__(f"upstream business error status={status}")
        self.status = status
        self.body = body


@dataclass(frozen=True)
class Behavior:
    """一次调用的脚本化行为。

    kind: ok / timeout / unavailable / error
    latency: 模拟耗时（秒），通过推进注入的时钟体现，无需真实睡眠。
    status/body: ok 或 error 时的响应内容。
    """

    kind: Literal["ok", "timeout", "unavailable", "error"]
    latency: float = 0.0
    status: int = 200
    body: dict = field(default_factory=dict)


class MockUpstream:
    """模拟上游：按脚本依次表现，脚本耗尽后重复最后一个行为。"""

    def __init__(
        self,
        upstream: Upstream,
        behaviors: list[Behavior] | None = None,
        clock: Clock | None = None,
        version: int = 1,
    ) -> None:
        self.upstream = upstream
        self._clock: Clock = clock or SystemClock()
        self._version = version
        self._behaviors: list[Behavior] = list(behaviors or [Behavior("ok")])
        self._lock = threading.Lock()
        self._calls = 0

    @property
    def call_count(self) -> int:
        with self._lock:
            return self._calls

    def set_behaviors(self, behaviors: list[Behavior]) -> None:
        with self._lock:
            self._behaviors = list(behaviors)

    def _next_behavior(self) -> Behavior:
        with self._lock:
            self._calls += 1
            if len(self._behaviors) > 1:
                return self._behaviors.pop(0)
            return self._behaviors[0]

    def call(self, request: GatewayRequest, timeout: float) -> GatewayResponse:
        behavior = self._next_behavior()
        if behavior.latency > 0:
            advance = getattr(self._clock, "advance", None)
            if callable(advance):
                advance(behavior.latency)
            else:
                import time

                time.sleep(behavior.latency)
        if behavior.kind == "timeout":
            raise UpstreamTimeoutFault(
                f"upstream '{self.upstream.name}' did not respond within {timeout}s"
            )
        if behavior.kind == "unavailable":
            raise UpstreamUnavailableFault(f"upstream '{self.upstream.name}' unavailable")
        if behavior.kind == "error":
            raise UpstreamBusinessFault(behavior.status, behavior.body)
        return GatewayResponse(
            status=behavior.status,
            body=behavior.body or {"upstream": self.upstream.name, "path": request.path},
            upstream=self.upstream.name,
            version=self._version,
        )
