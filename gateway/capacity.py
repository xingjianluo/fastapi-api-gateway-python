"""上游实例级共享容量保护：所有调用方共享同一上游的并发容量。

- 容量为硬上限，检查+占用在同一把锁内完成，并发下不超发；
- 调用方可配置预留额度（优先级）：预留未用满时高优先级直接放行；
  闲置预留允许其他调用方临时借用，高优先级回来后低优先级的新请求被拒，
  额度随在途请求完成而归还——在途请求绝不中途掐断；
- 规则以不可变快照整体替换；在途计数独立于规则，换规则不混账。

准入判定（acquire，全部在锁内）：
1. 总在途 >= capacity → 拒绝（硬上限，任何调用方都不例外）；
2. 调用方自身在途 < 其预留额度 → 放行（使用自己的预留槽）；
3. 否则走公共/借用通道：公共池 = capacity - 全部预留；其他调用方未使用的
   预留可临时借用。仅当 公共占用 < 公共池 + 当前可借用预留 时放行。
   高优先级回来占用预留后，可借用空间立即收缩，低优先级新请求被拒，
   额度随在途请求完成而归还——在途请求绝不中途掐断。
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from gateway.errors import ErrorCategory, GatewayError

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CapacityRule:
    """单个上游的容量规则。

    capacity: 该上游允许的最大并发在途请求数（所有调用方共享，硬上限）。
    reserved: 调用方 -> 预留并发数（高优先级保证额度）。
    """

    capacity: int
    reserved: Mapping[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.capacity < 1:
            raise ValueError("capacity>=1 required")
        if any(v < 0 for v in self.reserved.values()):
            raise ValueError("reserved values must be >= 0")
        if sum(self.reserved.values()) > self.capacity:
            raise ValueError("sum of reserved must not exceed capacity")


class UpstreamCapacityManager:
    """按上游维护在途计数与准入判定，线程安全。

    在途计数独立于规则快照：规则替换只影响后续 acquire 的判定，
    已在途请求的 release 照常记账，不会出现新旧规则混账。
    无规则的上游默认不限制（但仍记账，便于观测与规则热加载后的判定）。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._rules: MappingProxyType[str, CapacityRule] = MappingProxyType({})
        self._in_flight: dict[str, dict[str, int]] = {}

    def update_rules(self, rules: Mapping[str, CapacityRule]) -> None:
        """原子替换全部容量规则（快照语义）；在途计数保留。"""
        snapshot = MappingProxyType(
            {name: rule for name, rule in rules.items()}
        )
        with self._lock:
            self._rules = snapshot
        log.info("capacity rules updated: %s", {k: (v.capacity, dict(v.reserved)) for k, v in rules.items()})

    def acquire(self, caller_id: str, upstream: str) -> None:
        """占用一个容量槽；被容量拒绝时抛 UPSTREAM_CAPACITY_EXCEEDED（请求未发出）。"""
        with self._lock:
            per_caller = self._in_flight.setdefault(upstream, {})
            own = per_caller.get(caller_id, 0)
            total = sum(per_caller.values())
            rule = self._rules.get(upstream)
            if rule is not None:
                if total >= rule.capacity:
                    log.info(
                        "capacity hard limit upstream=%s caller=%s total=%d capacity=%d",
                        upstream, caller_id, total, rule.capacity,
                    )
                    raise self._reject(caller_id, upstream, rule, total)
                reserved = rule.reserved.get(caller_id, 0)
                if own >= reserved:
                    # 公共/借用通道：公共池 = capacity - 全部预留；
                    # 其他调用方未使用的预留（含尚未到场者）可临时借用，
                    # 预留一旦被实际占用，借用空间立即收缩
                    reserved_in_use = sum(
                        min(n, rule.reserved.get(c, 0))
                        for c, n in per_caller.items()
                    )
                    commons_used = total - reserved_in_use
                    commons_limit = rule.capacity - sum(rule.reserved.values())
                    borrowable = sum(
                        r - min(per_caller.get(c, 0), r)
                        for c, r in rule.reserved.items()
                        if c != caller_id
                    )
                    if commons_used >= commons_limit + borrowable:
                        log.info(
                            "capacity borrow denied upstream=%s caller=%s "
                            "commons_used=%d commons_limit=%d borrowable=%d",
                            upstream, caller_id, commons_used, commons_limit, borrowable,
                        )
                        raise self._reject(caller_id, upstream, rule, total)
            per_caller[caller_id] = own + 1

    def release(self, caller_id: str, upstream: str) -> None:
        """归还在途槽位；与 acquire 成对调用。"""
        with self._lock:
            per_caller = self._in_flight.get(upstream)
            if per_caller is None or per_caller.get(caller_id, 0) <= 0:
                raise RuntimeError(
                    f"unbalanced release: caller='{caller_id}' upstream='{upstream}'"
                )
            remaining = per_caller[caller_id] - 1
            if remaining:
                per_caller[caller_id] = remaining
            else:
                del per_caller[caller_id]

    def in_flight(self, upstream: str) -> int:
        """该上游当前在途总数（测试与观测用）。"""
        with self._lock:
            return sum(self._in_flight.get(upstream, {}).values())

    @staticmethod
    def _reject(
        caller_id: str, upstream: str, rule: CapacityRule, total: int
    ) -> GatewayError:
        return GatewayError(
            ErrorCategory.UPSTREAM_CAPACITY_EXCEEDED,
            f"upstream '{upstream}' capacity exceeded for caller='{caller_id}' "
            f"(in_flight={total}, capacity={rule.capacity})",
            retryable=True,
        )
