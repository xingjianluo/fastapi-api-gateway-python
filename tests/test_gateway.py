import logging

import pytest

from gateway.circuit import CircuitBreakerRegistry, CircuitConfig, CircuitState
from gateway.clock import ManualClock
from gateway.errors import ErrorCategory, GatewayError
from gateway.gateway import Gateway
from gateway.models import Caller, GatewayRequest, Method, Route, RouteVersion, Upstream
from gateway.ratelimit import RateLimiter
from gateway.router import Router
from gateway.upstreams import Behavior, MockUpstream

log = logging.getLogger(__name__)

UP_A = Upstream("svc-a")
UP_B = Upstream("svc-b")


def build(behaviors_a, behaviors_b, *, timeout=2.0):
    clock = ManualClock()
    router = Router(
        [
            Route(
                "get-item",
                Method.GET,
                "/items/{item_id}",
                (RouteVersion(1, (UP_A, UP_B)),),
            ),
            Route(
                "create-item",
                Method.POST,
                "/items",
                (RouteVersion(1, (UP_A, UP_B)),),
            ),
        ]
    )
    a = MockUpstream(UP_A, behaviors_a, clock)
    b = MockUpstream(UP_B, behaviors_b, clock)
    gw = Gateway(
        router,
        RateLimiter(clock),
        CircuitBreakerRegistry(CircuitConfig(3, 30.0, 1), clock),
        {"svc-a": a, "svc-b": b},
        timeout_seconds=timeout,
        clock=clock,
    )
    return gw, a, b, clock


def test_idempotent_request_fails_over_on_timeout():
    gw, a, b, _ = build([Behavior("timeout")], [Behavior("ok", body={"from": "b"})])
    req = GatewayRequest(Caller("c1"), Method.GET, "/items/1")
    resp = gw.handle(req)
    log.info("GET: a timeout -> failover to b, resp=%s a_calls=%d b_calls=%d",
             resp.body, a.call_count, b.call_count)
    assert resp.upstream == "svc-b"
    assert a.call_count == 1 and b.call_count == 1


def test_non_idempotent_request_never_duplicated():
    gw, a, b, _ = build([Behavior("timeout")], [Behavior("ok")])
    req = GatewayRequest(Caller("c1"), Method.POST, "/items", body=b"{}")
    with pytest.raises(GatewayError) as exc:
        gw.handle(req)
    log.info("POST: a timeout -> %s, a_calls=%d b_calls=%d",
             exc.value.category, a.call_count, b.call_count)
    assert exc.value.category is ErrorCategory.UPSTREAM_TIMEOUT
    assert a.call_count == 1
    assert b.call_count == 0  # 备用上游不得执行


def test_latency_over_timeout_detected_by_gateway():
    gw, a, b, clock = build([Behavior("ok", latency=3.0)], [Behavior("ok")], timeout=2.0)
    req = GatewayRequest(Caller("c1"), Method.GET, "/items/1")
    resp = gw.handle(req)
    log.info("a latency=3s > timeout=2s -> failover, resp upstream=%s", resp.upstream)
    assert resp.upstream == "svc-b"


def test_business_error_not_failed_over_and_normalized():
    gw, a, b, _ = build([Behavior("error", status=500, body={"msg": "boom"})], [Behavior("ok")])
    req = GatewayRequest(Caller("c1"), Method.GET, "/items/1")
    with pytest.raises(GatewayError) as exc:
        gw.handle(req)
    log.info("a 500 -> %s (no failover), b_calls=%d", exc.value.category, b.call_count)
    assert exc.value.category is ErrorCategory.UPSTREAM_ERROR
    assert b.call_count == 0


def test_circuit_open_skips_to_backup_even_for_non_idempotent():
    gw, a, b, _ = build([Behavior("unavailable")] * 3, [Behavior("unavailable")] * 3)
    # 先熔断 a（3 次连续失败；b 也不可用，请求整体失败）
    for _ in range(3):
        with pytest.raises(GatewayError):
            gw.handle(GatewayRequest(Caller("c1"), Method.GET, "/items/1"))
    b.set_behaviors([Behavior("ok", body={"from": "b"})])
    # 非幂等请求：a 熔断未发出请求，可安全走 b
    resp = gw.handle(GatewayRequest(Caller("c1"), Method.POST, "/items"))
    log.info("a circuit open -> POST routed to b=%s, a_calls=%d", resp.upstream, a.call_count)
    assert resp.upstream == "svc-b"
    assert a.call_count == 3


def test_circuit_state_transitions_through_gateway():
    clock = ManualClock()
    router = Router([Route("r", Method.GET, "/x", (RouteVersion(1, (UP_A,)),))])
    a = MockUpstream(UP_A, [Behavior("unavailable")], clock)
    circuits = CircuitBreakerRegistry(CircuitConfig(2, 30.0, 1), clock)
    gw = Gateway(router, RateLimiter(clock), circuits, {"svc-a": a}, clock=clock)
    req = GatewayRequest(Caller("c1"), Method.GET, "/x")
    for i in range(2):
        with pytest.raises(GatewayError) as exc:
            gw.handle(req)
        log.info("failure #%d category=%s state=%s", i + 1, exc.value.category, circuits.state(UP_A).value)
    assert circuits.state(UP_A) is CircuitState.OPEN
    with pytest.raises(GatewayError) as exc:
        gw.handle(req)
    assert exc.value.category is ErrorCategory.CIRCUIT_OPEN
    # 推进时钟进入半开，探测成功后恢复
    clock.advance(30.0)
    a.set_behaviors([Behavior("ok", body={"recovered": True})])
    resp = gw.handle(req)
    log.info("half-open probe -> resp=%s state=%s", resp.body, circuits.state(UP_A).value)
    assert resp.body == {"recovered": True}
    assert circuits.state(UP_A) is CircuitState.CLOSED
