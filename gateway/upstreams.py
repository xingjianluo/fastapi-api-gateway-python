"""仓库内模拟上游服务，可脚本化延迟、失败与业务错误。"""

from __future__ import annotations

from gateway.clock import Clock, SystemClock
from gateway.models import GatewayRequest, GatewayResponse, Upstream


class MockUpstream:
    """模拟上游：按脚本返回成功/超时/不可用/业务错误。"""

    def __init__(self, upstream: Upstream, clock: Clock | None = None) -> None:
        self.upstream = upstream
        self._clock: Clock = clock or SystemClock()

    def call(self, request: GatewayRequest, timeout: float) -> GatewayResponse:
        raise NotImplementedError
