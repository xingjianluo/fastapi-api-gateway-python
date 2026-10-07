"""网关级联动：灰度自动回退/分阶段恢复、容量拒绝与故障切换。"""

import logging

import pytest

from gateway.canary import CanaryConfig, CanaryController, CanaryMode
from gateway.capacity import CapacityManager, CapacityRule
from gateway.circuit import CircuitBreakerRegistry, CircuitConfig
from gateway.clock import ManualClock
from gateway.errors import ErrorCategory, GatewayError
from gateway.gateway import Gateway
from gateway.models import Caller, GatewayRequest, Method, Route, RouteVersion, Upstream
from gateway.ratelimit import RateLimiter
from gateway.router import Router
from gateway.upstreams import Behavior, MockUpstream
from gateway.weights import WeightManager, pick_weighted

log = logging.getLogger(__name__)

UP_A = Upstream("svc-a")  # v1 稳定版
UP_B = Upstream("svc-b")  # v2 灰度版
ROUTE_ID = "get-item"


def build(behaviors_a, behaviors_b, *, weights=None, with_canary=True, capacity=None):
    clock = ManualClock()
    route = Route(
        ROUTE_ID,
        Method.GET,
        "/items/{item_id}",
        (RouteVersion(1, (UP_A,)), RouteVersion(2, (UP_B,))),
    )
    router = Router([route])
    circuits = CircuitBreakerRegistry(CircuitConfig(2, 30.0, 1), clock)
    canary = None
    if with_canary:
        canary = CanaryController(
            CanaryConfig(recovery_seconds=30.0, stage_dwell_seconds=10.0, ramp=(0.1, 0.5, 1.0)),
            clock,
        )
        canary.register_version(ROUTE_ID, 1, ("svc-a",))
        canary.register_version(ROUTE_ID, 2, ("svc-b",))
        circuits.add_listener(canary.on_circuit_event)
    a = MockUpstream(UP_A, behaviors_a, clock, version=1)
    b = MockUpstream(UP_B, behaviors_b, clock, version=2)
    gw = Gateway(
        router,
        RateLimiter(clock),
        circuits,
        {"svc-a": a, "svc-b": b},
        timeout_seconds=2.0,
        clock=clock,
        weights=WeightManager(weights or {(ROUTE_ID, 1): 50, (ROUTE_ID, 2): 50}),
        capacity=capacity,
        canary=canary,
    )
    return gw, a, b, clock, canary


def caller_landing_on(version, weight_pairs, prefix="c"):
    i = 0
    while True:
        cid = f"{prefix}{i}"
        if pick_weighted(weight_pairs, ROUTE_ID, cid) == version:
            return cid
        i += 1


def get(gw, caller_id):
    return gw.handle(GatewayRequest(Caller(caller_id), Method.GET, "/items/1"))


def test_gray_version_auto_suppressed_after_repeated_failures():
    gw, a, b, clock, canary = build(
        [Behavior("ok")], [Behavior("unavailable"), Behavior("unavailable"), Behavior("ok")]
    )
    c2 = caller_landing_on(2, [(1, 50), (2, 50)])
    # 灰度版本连续失败达到熔断阈值
    for i in range(2):
        with pytest.raises(GatewayError) as exc:
            get(gw, c2)
        log.info("failure %d on v2 -> %s", i + 1, exc.value.category)
    assert canary.mode(ROUTE_ID, 2) is CanaryMode.SUPPRESSED
    # 全部调用方（含原本粘性到 v2 的）都落到稳定版
    upstreams = {get(gw, f"c{i}").upstream for i in range(50)}
    log.info("after suppression, 50 callers all land on: %s", upstreams)
    assert upstreams == {"svc-a"}


def test_staged_recovery_restores_traffic_gradually():
    gw, a, b, clock, canary = build(
        [Behavior("ok")], [Behavior("unavailable"), Behavior("unavailable"), Behavior("ok")]
    )
    c2 = caller_landing_on(2, [(1, 50), (2, 50)])
    for _ in range(2):
        with pytest.raises(GatewayError):
            get(gw, c2)
    assert canary.mode(ROUTE_ID, 2) is CanaryMode.SUPPRESSED

    callers = [f"c{i}" for i in range(200)]

    def assert_distribution(w1, w2, label):
        expected = {cid: ("svc-a" if pick_weighted([(1, w1), (2, w2)], ROUTE_ID, cid) == 1 else "svc-b")
                    for cid in callers}
        actual = {cid: get(gw, cid).upstream for cid in callers}
        n_b = sum(1 for v in actual.values() if v == "svc-b")
        log.info("%s: weights=(%d,%d) -> svc-b callers=%d, matches prediction=%s",
                 label, w1, w2, n_b, actual == expected)
        assert actual == expected

    clock.advance(30.0)  # 熔断恢复等待期满 -> 阶段 0：10% 权重
    assert_distribution(50, 5, "stage0")
    clock.advance(10.0)  # dwell 满 -> 阶段 1：50% 权重
    assert_distribution(50, 25, "stage1")
    clock.advance(20.0)  # 两个 dwell -> 恢复完成（求值时惰性推进到 NORMAL）
    assert_distribution(50, 50, "restored")
    assert canary.mode(ROUTE_ID, 2) is CanaryMode.NORMAL


def test_failed_probe_during_recovery_reverts_to_suppressed():
    gw, a, b, clock, canary = build(
        [Behavior("ok")],
        [Behavior("unavailable"), Behavior("unavailable"), Behavior("unavailable")],
    )
    c2 = caller_landing_on(2, [(1, 50), (2, 50)])
    for _ in range(2):
        with pytest.raises(GatewayError):
            get(gw, c2)
    clock.advance(30.0)  # 进入恢复阶段 0，放试探流量
    probe = caller_landing_on(2, [(1, 50), (2, 5)])
    with pytest.raises(GatewayError) as exc:
        get(gw, probe)  # 半开探测失败
    log.info("probe during recovery failed -> %s", exc.value.category)
    assert canary.mode(ROUTE_ID, 2) is CanaryMode.SUPPRESSED
    upstreams = {get(gw, f"c{i}").upstream for i in range(50)}
    log.info("re-suppressed, all traffic back to: %s", upstreams)
    assert upstreams == {"svc-a"}


def test_capacity_rejection_fails_over_and_is_distinct():
    clock = ManualClock()
    capacity = CapacityManager(reclaim_window_seconds=5.0, clock=clock)
    capacity.update_rules({"svc-a": CapacityRule(1), "svc-b": CapacityRule(1)})
    # 单版本、主备两个上游：容量拒绝应像熔断拒绝一样切换到备上游
    route = Route(ROUTE_ID, Method.GET, "/items/{item_id}",
                  (RouteVersion(1, (UP_A, UP_B)),))
    a = MockUpstream(UP_A, [Behavior("ok")], clock, version=1)
    b = MockUpstream(UP_B, [Behavior("ok")], clock, version=1)
    gw = Gateway(
        Router([route]),
        RateLimiter(clock),
        CircuitBreakerRegistry(CircuitConfig(2, 30.0, 1), clock),
        {"svc-a": a, "svc-b": b},
        timeout_seconds=2.0,
        clock=clock,
        capacity=capacity,
    )
    # 模拟别的在途请求占满 svc-a
    held = capacity.acquire("svc-a", "other")
    resp = get(gw, "c0")  # 容量拒绝=请求未发出 -> 切换到 svc-b
    log.info("svc-a full -> failed over to %s", resp.upstream)
    assert resp.upstream == "svc-b"
    # 两个上游都满 -> 拒绝原因可与限流区分
    held2 = capacity.acquire("svc-b", "other")
    with pytest.raises(GatewayError) as exc:
        get(gw, "c0")
    log.info("both full -> %s retryable=%s", exc.value.category, exc.value.retryable)
    assert exc.value.category is ErrorCategory.UPSTREAM_CAPACITY_EXHAUSTED
    assert exc.value.category not in (ErrorCategory.RATE_LIMITED, ErrorCategory.QUOTA_EXHAUSTED)
    # 在途请求收尾归还额度后恢复
    held.release()
    held2.release()
    assert get(gw, "c0").upstream == "svc-a"
