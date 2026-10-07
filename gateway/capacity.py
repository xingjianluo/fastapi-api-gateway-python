"""上游实例级共享并发容量：所有调用方共享，按优先级预留 + 可借用 + 可回收。

语义：
- 容量按上游实例统计在途请求数（acquire/release），在途请求不被中途掐断；
- 每个优先级有预留额度（reserved），占用自有预留始终优先；
- 预留未用满的部分允许其他优先级临时借用（borrow），但总数不超过 total_slots；
- 高优先级回来而额度被借满时，其请求被拒（retryable）并记录一个回收标记
  （reclaim，有效期 reclaim_window_seconds）；标记有效期内更低优先级的
  新借用请求一律被拒，把释放出来的额度让给高优先级；在途请求不受影响，
  正常收尾后额度自然归还。高优先级占回自己的预留后标记清除；
- 规则快照式整体替换；在途计数按 (上游, 优先级) 独立记账，不受规则替换影响；
- 无规则的上游默认放行。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from types import MappingProxyType

from gateway.clock import Clock, SystemClock
from gateway.errors import ErrorCategory, GatewayError

DEFAULT_PRIORITY = 0


@dataclass(frozen=True)
class CapacityRule:
    """单个上游实例的容量规则。

    total_slots: 该上游允许的最大在途请求数（所有调用方共享）。
    reserved: 优先级 -> 预留额度；sum(reserved) <= total_slots。
    """

    total_slots: int
    reserved: MappingProxyType[int, int]

    def __init__(self, total_slots: int, reserved: dict[int, int] | None = None) -> None:
        reserved = dict(reserved or {})
        if total_slots < 1:
            raise ValueError("total_slots must be >= 1")
        if any(v < 0 for v in reserved.values()):
            raise ValueError("reserved slots must be >= 0")
        if sum(reserved.values()) > total_slots:
            raise ValueError("sum of reserved slots must not exceed total_slots")
        object.__setattr__(self, "total_slots", total_slots)
        object.__setattr__(self, "reserved", MappingProxyType(reserved))


class CapacityPermit:
    """一次容量占用的凭证；release 幂等，规则变更后仍可正确归还。"""

    __slots__ = ("_manager", "_key", "_released")

    def __init__(self, manager: "CapacityManager", key: tuple[str, int] | None) -> None:
        self._manager = manager
        self._key = key  # None 表示无规则放行，无需记账
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        if self._key is not None:
            self._manager._release(self._key)

    def __enter__(self) -> "CapacityPermit":
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()


class _Config:
    """不可变配置快照。"""

    __slots__ = ("rules", "caller_priority")

    def __init__(
        self,
        rules: dict[str, CapacityRule],
        caller_priority: dict[str, int],
    ) -> None:
        self.rules = MappingProxyType(dict(rules))
        self.caller_priority = MappingProxyType(dict(caller_priority))


class CapacityManager:
    """按上游实例的共享容量管理器，线程安全，时间由注入时钟驱动。

    准入判定（同一把锁内完成检查+计数，并发下不超发）：
    1. 本优先级在途 < 本优先级预留 且 总在途 < total_slots -> 占用自有预留；
    2. 否则若 总在途 < total_slots 且 无更高优先级的有效回收标记 -> 借用；
    3. 否则拒绝（UPSTREAM_CAPACITY_EXHAUSTED，retryable）；
       若被拒时本优先级在途 < 预留（自有额度被借走），记录回收标记。
    """

    def __init__(
        self,
        reclaim_window_seconds: float = 5.0,
        clock: Clock | None = None,
    ) -> None:
        if reclaim_window_seconds <= 0:
            raise ValueError("reclaim_window_seconds must be > 0")
        self._reclaim_window = reclaim_window_seconds
        self._clock: Clock = clock or SystemClock()
        self._lock = threading.Lock()
        self._config = _Config({}, {})
        self._in_flight: dict[tuple[str, int], int] = {}
        self._reclaim_until: dict[tuple[str, int], float] = {}

    def update_rules(
        self,
        rules: dict[str, CapacityRule],
        caller_priority: dict[str, int] | None = None,
    ) -> None:
        """原子替换容量规则与调用方优先级映射（快照语义）。

        在途计数按 (上游, 优先级) 独立保存，不随快照替换清零，
        因此在途请求的 release 在规则变更后仍然归还到正确的桶。
        """
        config = _Config(rules, caller_priority or {})
        with self._lock:
            self._config = config

    def acquire(self, upstream: str, caller_id: str) -> CapacityPermit:
        """占用一个在途名额；容量不足抛 UPSTREAM_CAPACITY_EXHAUSTED。"""
        with self._lock:
            config = self._config
            rule = config.rules.get(upstream)
            if rule is None:
                return CapacityPermit(self, None)
            priority = config.caller_priority.get(caller_id, DEFAULT_PRIORITY)
            key = (upstream, priority)
            now = self._clock.now()
            total = self._total_in_flight(upstream)
            used = self._in_flight.get(key, 0)
            reserved = rule.reserved.get(priority, 0)
            if used >= reserved:
                # 已占回全部自有预留，回收标记失效
                self._reclaim_until.pop(key, None)
            if used < reserved and total < rule.total_slots:
                self._in_flight[key] = used + 1
                return CapacityPermit(self, key)
            if total < rule.total_slots and not self._blocked_by_reclaim(
                upstream, priority, rule, now
            ):
                self._in_flight[key] = used + 1
                return CapacityPermit(self, key)
            if used < reserved:
                # 自有预留被借走且容量已满：记录回收标记，要求低优先级让路
                self._reclaim_until[key] = now + self._reclaim_window
            raise GatewayError(
                ErrorCategory.UPSTREAM_CAPACITY_EXHAUSTED,
                f"upstream '{upstream}' shared capacity exhausted "
                f"(caller='{caller_id}' priority={priority} "
                f"in_flight={total}/{rule.total_slots})",
                retryable=True,
            )

    def _total_in_flight(self, upstream: str) -> int:
        return sum(n for (u, _), n in self._in_flight.items() if u == upstream)

    def _blocked_by_reclaim(
        self, upstream: str, priority: int, rule: CapacityRule, now: float
    ) -> bool:
        """存在更高优先级且未占回预留的有效回收标记时，本优先级的借用须让路。"""
        for (u, q), deadline in self._reclaim_until.items():
            if u != upstream or q <= priority or deadline <= now:
                continue
            if self._in_flight.get((u, q), 0) < rule.reserved.get(q, 0):
                return True
        return False

    def _release(self, key: tuple[str, int]) -> None:
        with self._lock:
            remaining = self._in_flight.get(key, 0) - 1
            if remaining <= 0:
                self._in_flight.pop(key, None)
            else:
                self._in_flight[key] = remaining

    def in_flight(self, upstream: str, priority: int) -> int:
        """观测用：某上游某优先级当前在途数。"""
        with self._lock:
            return self._in_flight.get((upstream, priority), 0)
