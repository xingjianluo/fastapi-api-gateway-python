"""按调用方+路由维度的限流与配额控制。"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass
from types import MappingProxyType

from gateway.clock import Clock, SystemClock
from gateway.errors import ErrorCategory, GatewayError


@dataclass(frozen=True)
class RateLimitRule:
    """单条限流规则。

    burst_limit: 每个 window_seconds 固定窗口内允许的最大请求数（突发上限）。
    quota: 该 (调用方, 路由) 维度累计允许的最大请求数（长期配额）。
    """

    burst_limit: int
    window_seconds: float
    quota: int

    def __post_init__(self) -> None:
        if self.burst_limit < 1 or self.window_seconds <= 0 or self.quota < 1:
            raise ValueError("burst_limit>=1, window_seconds>0, quota>=1 required")


class _Counter:
    __slots__ = ("window_start", "window_count", "total")

    def __init__(self) -> None:
        self.window_start = -1.0
        self.window_count = 0
        self.total = 0


class RateLimiter:
    """原子计数限流器。

    - 计数在同一把锁内完成“检查+递增”，高并发下不超发。
    - 规则以不可变快照整体替换，读取方要么看到旧快照要么看到新快照，
      不存在新旧配置混合生效的中间状态。
    - 无规则的 (调用方, 路由) 默认放行。
    """

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock: Clock = clock or SystemClock()
        self._lock = threading.Lock()
        self._rules: MappingProxyType[tuple[str, str], RateLimitRule] = MappingProxyType({})
        self._counters: dict[tuple[str, str], _Counter] = {}

    def update_rules(self, rules: dict[tuple[str, str], RateLimitRule]) -> None:
        """原子替换全部规则（快照语义）。"""
        snapshot = MappingProxyType(dict(rules))
        with self._lock:
            self._rules = snapshot

    def check(self, caller_id: str, route_id: str) -> None:
        """通过则计数；拒绝时抛 RATE_LIMITED（突发超限）或 QUOTA_EXHAUSTED（配额耗尽）。"""
        key = (caller_id, route_id)
        with self._lock:
            rule = self._rules.get(key)
            if rule is None:
                return
            counter = self._counters.setdefault(key, _Counter())
            now = self._clock.now()
            window_start = math.floor(now / rule.window_seconds) * rule.window_seconds
            if counter.window_start != window_start:
                counter.window_start = window_start
                counter.window_count = 0
            if counter.total >= rule.quota:
                raise GatewayError(
                    ErrorCategory.QUOTA_EXHAUSTED,
                    f"quota exhausted for caller='{caller_id}' route='{route_id}' "
                    f"(quota={rule.quota})",
                    retryable=False,
                )
            if counter.window_count >= rule.burst_limit:
                raise GatewayError(
                    ErrorCategory.RATE_LIMITED,
                    f"burst limit exceeded for caller='{caller_id}' route='{route_id}' "
                    f"({rule.burst_limit}/{rule.window_seconds}s)",
                    retryable=True,
                )
            counter.window_count += 1
            counter.total += 1
