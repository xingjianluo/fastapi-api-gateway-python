"""权重分流：确定性、调用方粘性、运行期调整、权重 0 摘流。"""

import logging

from gateway.models import RouteVersion, Upstream
from gateway.weights import VersionWeighter

log = logging.getLogger(__name__)

ROUTE = "route-a"
VERSIONS = (
    RouteVersion(1, (Upstream("up-v1"),)),
    RouteVersion(2, (Upstream("up-v2"),)),
)


def make_weighter(weights: dict[int, int]) -> VersionWeighter:
    weighter = VersionWeighter()
    weighter.update_weights(ROUTE, weights)
    return weighter


def effective(weighter: VersionWeighter) -> dict[int, int]:
    return dict(weighter.configured(ROUTE))


def test_same_caller_is_sticky_while_weights_unchanged():
    weighter = make_weighter({1: 50, 2: 50})
    picks = {
        weighter.select(ROUTE, "caller-1", VERSIONS, effective(weighter)).version
        for _ in range(20)
    }
    log.info("caller-1 picked %s across 20 selections with unchanged weights", picks)
    assert len(picks) == 1


def test_selection_is_deterministic_across_instances():
    weights = {1: 30, 2: 70}
    callers = [f"caller-{i}" for i in range(50)]
    first = [
        make_weighter(weights).select(ROUTE, c, VERSIONS, weights).version for c in callers
    ]
    second = [
        make_weighter(weights).select(ROUTE, c, VERSIONS, weights).version for c in callers
    ]
    log.info("50 callers, two independent weighters agree=%s", first == second)
    assert first == second


def test_zero_weight_version_gets_no_traffic():
    weighter = make_weighter({1: 100, 2: 0})
    for i in range(30):
        chosen = weighter.select(ROUTE, f"caller-{i}", VERSIONS, effective(weighter))
        assert chosen.version == 1
    log.info("weights={1:100, 2:0}: 30 callers all landed on v1")


def test_weight_update_takes_effect_for_new_selections():
    weighter = make_weighter({1: 100, 2: 0})
    before = weighter.select(ROUTE, "caller-x", VERSIONS, effective(weighter)).version
    # 运行期整体替换快照：v1 摘流、v2 全量
    weighter.update_weights(ROUTE, {1: 0, 2: 100})
    after = weighter.select(ROUTE, "caller-x", VERSIONS, effective(weighter)).version
    log.info("caller-x before update -> v%d, after {1:0, 2:100} -> v%d", before, after)
    assert before == 1
    assert after == 2


def test_all_zero_weights_returns_none():
    weighter = make_weighter({1: 0, 2: 0})
    chosen = weighter.select(ROUTE, "caller-1", VERSIONS, effective(weighter))
    log.info("weights all zero -> select returned %s", chosen)
    assert chosen is None


def test_distribution_roughly_follows_weights():
    weights = {1: 80, 2: 20}
    weighter = make_weighter(weights)
    eff = effective(weighter)
    count_v2 = sum(
        1
        for i in range(2000)
        if weighter.select(ROUTE, f"caller-{i}", VERSIONS, eff).version == 2
    )
    ratio = count_v2 / 2000
    log.info("weights 80/20 over 2000 callers -> v2 ratio=%.3f", ratio)
    assert 0.10 < ratio < 0.30


# ---- 网关编排集成：权重 0 摘流、运行期调整立即生效 ----

from gateway.circuit import CircuitBreakerRegistry
from gateway.clock import ManualClock
from gateway.gateway import Gateway
from gateway.models import Caller, GatewayRequest, Method, Route, RouteVersion, Upstream
from gateway.ratelimit import RateLimiter
from gateway.router import Router
from gateway.upstreams import Behavior, MockUpstream

U1 = Upstream("svc-v1")
U2 = Upstream("svc-v2")
GW_ROUTE = Route(
    "get-item", Method.GET, "/items/{i}",
    (RouteVersion(1, (U1,)), RouteVersion(2, (U2,))),
)


def _gateway_with_weights(weights: dict[int, int]):
    clock = ManualClock()
    weighter = VersionWeighter()
    weighter.update_weights("get-item", weights)
    up1 = MockUpstream(U1, [Behavior("ok", body={"svc": "v1"})], clock, version=1)
    up2 = MockUpstream(U2, [Behavior("ok", body={"svc": "v2"})], clock, version=2)
    gateway = Gateway(
        Router([GW_ROUTE]),
        RateLimiter(clock),
        CircuitBreakerRegistry(clock=clock),
        {"svc-v1": up1, "svc-v2": up2},
        clock=clock,
        weighter=weighter,
    )
    return gateway, weighter, up1, up2


def test_weight_zero_version_gets_no_traffic():
    gateway, _, up1, up2 = _gateway_with_weights({1: 100, 2: 0})
    for i in range(20):
        resp = gateway.handle(GatewayRequest(Caller(f"c-{i}"), Method.GET, "/items/1"))
        assert resp.version == 1
    log.info("weights={1:100, 2:0}: v2 call_count=%d", up2.call_count)
    assert up2.call_count == 0
    assert up1.call_count == 20


def test_weight_update_applies_immediately_to_new_requests():
    gateway, weighter, up1, up2 = _gateway_with_weights({1: 100, 2: 0})
    gateway.handle(GatewayRequest(Caller("c-0"), Method.GET, "/items/1"))
    assert up2.call_count == 0
    # 运行期整体替换权重：新请求立即按新快照判定
    weighter.update_weights("get-item", {1: 0, 2: 100})
    resp = gateway.handle(GatewayRequest(Caller("c-0"), Method.GET, "/items/1"))
    log.info("after weight swap {1:0, 2:100}: c-0 -> version=%d", resp.version)
    assert resp.version == 2
    assert up2.call_count == 1


def test_caller_stays_on_same_version_while_weights_unchanged():
    gateway, _, _, _ = _gateway_with_weights({1: 50, 2: 50})
    seen = {
        gateway.handle(GatewayRequest(Caller("sticky"), Method.GET, "/items/1")).version
        for _ in range(10)
    }
    log.info("caller 'sticky' saw versions=%s across 10 requests", seen)
    assert len(seen) == 1
