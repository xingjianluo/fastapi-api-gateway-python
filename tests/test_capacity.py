"""上游共享容量：并发原子性、预留占用/借用/归还、规则快照替换。"""

import logging
import threading

import pytest

from gateway.capacity import CapacityRule, UpstreamCapacityManager
from gateway.errors import ErrorCategory, GatewayError

log = logging.getLogger(__name__)

UP = "up-a"


def make_manager(capacity: int, reserved: dict[str, int] | None = None) -> UpstreamCapacityManager:
    manager = UpstreamCapacityManager()
    manager.update_rules({UP: CapacityRule(capacity=capacity, reserved=reserved or {})})
    return manager


def test_hard_capacity_never_exceeded_under_concurrency():
    manager = make_manager(capacity=20)
    acquired = 0
    lock = threading.Lock()

    def worker(caller: str) -> None:
        nonlocal acquired
        for _ in range(10):
            try:
                manager.acquire(caller, UP)
                with lock:
                    acquired += 1
            except GatewayError:
                pass

    threads = [threading.Thread(target=worker, args=(f"caller-{i}",)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log.info("capacity=20, 10 threads x 10 acquires -> acquired=%d in_flight=%d",
             acquired, manager.in_flight(UP))
    assert acquired == 20
    assert manager.in_flight(UP) == 20


def test_release_is_balanced_and_rejects_double_release():
    manager = make_manager(capacity=5)
    manager.acquire("a", UP)
    manager.release("a", UP)
    assert manager.in_flight(UP) == 0
    with pytest.raises(RuntimeError):
        manager.release("a", UP)
    log.info("double release -> RuntimeError, in_flight stays 0")


def test_reserved_caller_admitted_while_borrower_rejected():
    manager = make_manager(capacity=4, reserved={"vip": 2})
    # 低优先级借用：vip 预留未被占用时，low 最多用到 capacity（含借用预留）
    manager.acquire("low", UP)
    manager.acquire("low", UP)
    manager.acquire("low", UP)
    manager.acquire("low", UP)
    log.info("low borrowed up to hard cap, in_flight=%d", manager.in_flight(UP))
    with pytest.raises(GatewayError) as exc:
        manager.acquire("low", UP)
    assert exc.value.category is ErrorCategory.UPSTREAM_CAPACITY_EXCEEDED
    # 硬上限下 vip 也只能等在途释放；释放一个后 vip 用预留槽进入
    manager.release("low", UP)
    manager.acquire("vip", UP)
    manager.release("low", UP)
    manager.acquire("vip", UP)
    log.info("vip in_flight=2 (within reservation), low in_flight=2")
    # vip 预留已用满 2 槽：low 的新请求必须让路（total=4 >= 4-2 不成立即拒）
    with pytest.raises(GatewayError) as exc2:
        manager.acquire("low", UP)
    assert exc2.value.category is ErrorCategory.UPSTREAM_CAPACITY_EXCEEDED
    log.info("low rejected while vip reservation in use: %s", exc2.value.message)


def test_borrowed_capacity_returns_as_in_flight_drains():
    manager = make_manager(capacity=3, reserved={"vip": 2})
    manager.acquire("low", UP)
    manager.acquire("low", UP)
    manager.acquire("low", UP)  # 借满
    # vip 回来：在途不被掐断，新请求等 low 释放后逐个拿回预留槽
    manager.release("low", UP)
    manager.acquire("vip", UP)
    manager.release("low", UP)
    manager.acquire("vip", UP)
    # vip 占满预留 2 槽后，low 只剩 capacity-reserved=1 个公共槽可用
    manager.release("low", UP)
    manager.acquire("low", UP)
    with pytest.raises(GatewayError):
        manager.acquire("low", UP)
    log.info("after drain: vip=2 (reserved), low=1 (commons), further low rejected")


def test_capacity_rejection_category_is_distinct():
    manager = make_manager(capacity=1)
    manager.acquire("a", UP)
    with pytest.raises(GatewayError) as exc:
        manager.acquire("b", UP)
    log.info("capacity rejection -> %s (retryable=%s)",
             exc.value.category, exc.value.retryable)
    assert exc.value.category is ErrorCategory.UPSTREAM_CAPACITY_EXCEEDED
    assert exc.value.category not in (ErrorCategory.RATE_LIMITED, ErrorCategory.QUOTA_EXHAUSTED)
    assert exc.value.retryable is True


def test_rule_update_is_atomic_and_in_flight_accounting_survives():
    manager = make_manager(capacity=2)
    manager.acquire("a", UP)
    manager.acquire("a", UP)
    # 快照整体替换：容量收紧到 1，但在途 2 个请求不被掐断
    manager.update_rules({UP: CapacityRule(capacity=1)})
    with pytest.raises(GatewayError):
        manager.acquire("a", UP)
    manager.release("a", UP)
    manager.release("a", UP)
    assert manager.in_flight(UP) == 0
    # 新快照生效：现在可进 1 个
    manager.acquire("a", UP)
    with pytest.raises(GatewayError):
        manager.acquire("a", UP)
    log.info("rules swapped mid-flight: in-flight drained cleanly, new capacity=1 enforced")


def test_no_rule_allows_unlimited():
    manager = UpstreamCapacityManager()
    for _ in range(100):
        manager.acquire("a", "unmanaged")
    assert manager.in_flight("unmanaged") == 100
    log.info("upstream without rule: 100 acquires all admitted")


# ---- 网关编排集成：容量拒绝参与故障切换，拒绝类别可区分 ----

from gateway.clock import ManualClock
from gateway.circuit import CircuitBreakerRegistry
from gateway.gateway import Gateway
from gateway.models import Caller, GatewayRequest, Method, Route, RouteVersion, Upstream
from gateway.ratelimit import RateLimiter, RateLimitRule
from gateway.router import Router
from gateway.upstreams import Behavior, MockUpstream

UA = Upstream("up-a")
UB = Upstream("up-b")
ROUTE = Route("get-item", Method.GET, "/items/{i}", (RouteVersion(1, (UA, UB)),))


def _gateway_with_capacity(
    capacity: UpstreamCapacityManager, rate_rules: dict | None = None
) -> tuple[Gateway, MockUpstream, MockUpstream]:
    clock = ManualClock()
    limiter = RateLimiter(clock)
    if rate_rules:
        limiter.update_rules(rate_rules)
    up_a = MockUpstream(UA, [Behavior("ok", body={"svc": "a"})], clock)
    up_b = MockUpstream(UB, [Behavior("ok", body={"svc": "b"})], clock)
    gateway = Gateway(
        Router([ROUTE]),
        limiter,
        CircuitBreakerRegistry(clock=clock),
        {"up-a": up_a, "up-b": up_b},
        clock=clock,
        capacity=capacity,
    )
    return gateway, up_a, up_b


def test_capacity_rejection_fails_over_to_backup_upstream():
    capacity = UpstreamCapacityManager()
    capacity.update_rules({"up-a": CapacityRule(capacity=1)})
    gateway, up_a, up_b = _gateway_with_capacity(capacity)
    # 别的调用方占满 up-a 的唯一槽位（模拟在途）
    capacity.acquire("other", "up-a")
    resp = gateway.handle(GatewayRequest(Caller("me"), Method.GET, "/items/1"))
    log.info("up-a full -> failover to up-b: upstream=%s", resp.upstream)
    assert resp.upstream == "up-b"
    assert up_a.call_count == 0  # 容量拒绝意味着请求未发出
    assert up_b.call_count == 1


def test_capacity_rejection_category_distinct_from_rate_limit():
    capacity = UpstreamCapacityManager()
    capacity.update_rules(
        {"up-a": CapacityRule(capacity=1), "up-b": CapacityRule(capacity=1)}
    )
    gateway, _, _ = _gateway_with_capacity(
        capacity,
        rate_rules={("limited", "get-item"): RateLimitRule(1, 60.0, 100)},
    )
    capacity.acquire("other", "up-a")
    capacity.acquire("other", "up-b")
    # 两个上游容量都满 -> upstream_capacity_exceeded
    with pytest.raises(GatewayError) as exc:
        gateway.handle(GatewayRequest(Caller("me"), Method.GET, "/items/1"))
    log.info("both upstreams full -> %s", exc.value.category)
    assert exc.value.category is ErrorCategory.UPSTREAM_CAPACITY_EXCEEDED
    # 释放容量后，同一网关下调用方自身突发超限仍是 rate_limited，二者可区分
    capacity.release("other", "up-a")
    capacity.release("other", "up-b")
    gateway.handle(GatewayRequest(Caller("limited"), Method.GET, "/items/1"))
    with pytest.raises(GatewayError) as exc2:
        gateway.handle(GatewayRequest(Caller("limited"), Method.GET, "/items/1"))
    log.info("caller burst exceeded -> %s", exc2.value.category)
    assert exc2.value.category is ErrorCategory.RATE_LIMITED


def test_in_flight_released_after_call_completes():
    capacity = UpstreamCapacityManager()
    capacity.update_rules({"up-a": CapacityRule(capacity=1)})
    gateway, _, _ = _gateway_with_capacity(capacity)
    gateway.handle(GatewayRequest(Caller("me"), Method.GET, "/items/1"))
    log.info("after completed call in_flight(up-a)=%d", capacity.in_flight("up-a"))
    assert capacity.in_flight("up-a") == 0  # 完成后槽位归还，下一请求可再进
    gateway.handle(GatewayRequest(Caller("me"), Method.GET, "/items/1"))
