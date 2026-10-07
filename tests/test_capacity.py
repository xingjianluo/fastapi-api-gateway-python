"""上游共享容量：并发原子性、优先级预留的占用/借用/归还、规则快照替换。"""

import logging
import threading

import pytest

from gateway.capacity import CapacityManager, CapacityRule
from gateway.clock import ManualClock
from gateway.errors import ErrorCategory, GatewayError

log = logging.getLogger(__name__)

UP = "svc-a"


def make_manager(total=4, reserved=None, priorities=None, window=5.0):
    clock = ManualClock()
    manager = CapacityManager(reclaim_window_seconds=window, clock=clock)
    manager.update_rules({UP: CapacityRule(total, reserved or {})}, priorities or {})
    return manager, clock


def test_no_rule_defaults_to_allow():
    manager = CapacityManager()
    permits = [manager.acquire("unconfigured", "c") for _ in range(1000)]
    log.info("no rule -> 1000 concurrent admits")
    assert len(permits) == 1000


def test_total_capacity_is_enforced_under_concurrency():
    manager, _ = make_manager(total=8)
    admitted = []
    rejected = []
    lock = threading.Lock()

    def worker(i):
        try:
            p = manager.acquire(UP, f"c{i}")
            with lock:
                admitted.append(p)
        except GatewayError as exc:
            with lock:
                rejected.append(exc.category)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(64)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log.info("total=8, 64 concurrent -> admitted=%d rejected=%d", len(admitted), len(rejected))
    assert len(admitted) == 8
    assert len(rejected) == 56
    assert set(rejected) == {ErrorCategory.UPSTREAM_CAPACITY_EXHAUSTED}
    # 不超发：在途数恰好等于总容量
    assert manager.in_flight(UP, 0) == 8


def test_rejection_category_is_distinct_from_rate_limit():
    manager, _ = make_manager(total=1)
    manager.acquire(UP, "c0")
    with pytest.raises(GatewayError) as exc:
        manager.acquire(UP, "c1")
    log.info("capacity reject -> %s (rate_limited=%s quota_exhausted=%s)",
             exc.value.category, ErrorCategory.RATE_LIMITED, ErrorCategory.QUOTA_EXHAUSTED)
    assert exc.value.category is ErrorCategory.UPSTREAM_CAPACITY_EXHAUSTED
    assert exc.value.category not in (ErrorCategory.RATE_LIMITED, ErrorCategory.QUOTA_EXHAUSTED)
    assert exc.value.retryable is True


def test_low_priority_borrows_unused_reservation():
    manager, _ = make_manager(total=4, reserved={1: 2}, priorities={"vip": 1})
    permits = [manager.acquire(UP, f"c{i}") for i in range(4)]
    log.info("reserved{1:2} unused -> low priority borrowed to %d/4", manager.in_flight(UP, 0))
    assert manager.in_flight(UP, 0) == 4
    for p in permits:
        p.release()


def test_reclaim_makes_low_priority_yield_and_high_priority_recover():
    manager, clock = make_manager(total=4, reserved={1: 2}, priorities={"vip": 1}, window=5.0)
    low = [manager.acquire(UP, f"c{i}") for i in range(4)]

    # 高优先级回来，容量被借满 -> 被拒并记录回收标记
    with pytest.raises(GatewayError) as exc:
        manager.acquire(UP, "vip")
    log.info("vip returns to full upstream -> %s, reclaim marked", exc.value.category)

    # 在途请求不被掐断：低优先级正常收尾
    low[0].release()
    log.info("one low-priority request finished in flight (not preempted)")

    # 回收标记有效期内，低优先级新借用请求让路
    with pytest.raises(GatewayError) as exc2:
        manager.acquire(UP, "cX")
    log.info("low-priority borrow during reclaim -> %s", exc2.value.category)
    assert exc2.value.category is ErrorCategory.UPSTREAM_CAPACITY_EXHAUSTED

    # 高优先级占回自有预留
    vip1 = manager.acquire(UP, "vip")
    low[1].release()
    vip2 = manager.acquire(UP, "vip")
    log.info("vip reclaimed reserved slots, in_flight p1=%d", manager.in_flight(UP, 1))
    assert manager.in_flight(UP, 1) == 2

    # 回收标记过期后，低优先级可再次借用空闲额度
    clock.advance(6.0)
    low[2].release()
    extra = manager.acquire(UP, "cY")
    log.info("after reclaim window expired, low borrows again; total=%d",
             manager.in_flight(UP, 0) + manager.in_flight(UP, 1))
    for p in (vip1, vip2, extra, low[3]):
        p.release()
    assert manager.in_flight(UP, 0) == 0 and manager.in_flight(UP, 1) == 0


def test_release_is_idempotent():
    manager, _ = make_manager(total=2)
    p = manager.acquire(UP, "c0")
    p.release()
    p.release()
    log.info("double release -> in_flight=%d", manager.in_flight(UP, 0))
    assert manager.in_flight(UP, 0) == 0
    # 额度未被重复归还（否则能超发）
    manager.acquire(UP, "c1")
    manager.acquire(UP, "c2")
    with pytest.raises(GatewayError):
        manager.acquire(UP, "c3")


def test_in_flight_accounting_survives_rule_update():
    manager, _ = make_manager(total=2)
    p1 = manager.acquire(UP, "c0")
    # 规则整体替换（缩容到 1），在途请求的账不能记混
    manager.update_rules({UP: CapacityRule(1)}, {})
    with pytest.raises(GatewayError):
        manager.acquire(UP, "c1")  # 新规则 total=1，在途 1 -> 拒
    p1.release()  # 在途请求按旧判定进入，仍正确归还
    log.info("after rule update + release, in_flight=%d", manager.in_flight(UP, 0))
    assert manager.in_flight(UP, 0) == 0
    p2 = manager.acquire(UP, "c2")  # 归还后新规则下放行
    p2.release()


def test_reserved_sum_cannot_exceed_total():
    with pytest.raises(ValueError):
        CapacityRule(2, {1: 2, 2: 1})
    log.info("reserved sum > total -> ValueError")
