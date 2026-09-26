"""按调用方+路由维度的限流与配额控制。"""

from __future__ import annotations

from dataclasses import dataclass

from gateway.clock import Clock, SystemClock
from gateway.errors import GatewayError


@dataclass(frozen=True)
class RateLimitRule:
    """单条限流规则：窗口内允许的突发上限与长期配额。"""

    burst_limit: int
    window_seconds: float
    quota: int


class RateLimiter:
    """原子计数限流器；规则整体替换，不存在新旧混合的中间状态。"""

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock: Clock = clock or SystemClock()

    def update_rules(self, rules: dict[tuple[str, str], RateLimitRule]) -> None:
        """原子替换全部规则。"""
        raise NotImplementedError

    def check(self, caller_id: str, route_id: str) -> None:
        """通过则计数；拒绝时抛 RATE_LIMITED 或 QUOTA_EXHAUSTED。"""
        raise NotImplementedError
