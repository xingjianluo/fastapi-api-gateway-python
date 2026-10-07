"""灰度与健康保护联动：自动回退、分阶段恢复、探测失败退回、结果可复现。"""

import logging

from gateway.canary import CanaryConfig, CanaryController, CanaryState
from gateway.circuit import CircuitBreakerRegistry, CircuitConfig
from gateway.clock import ManualClock
from gateway.gateway import Gateway
from gateway.models import Caller, GatewayRequest, Method, Route, RouteVersion, Upstream
from gateway.ratelimit import RateLimiter
from gateway.router import Router
from gateway.upstreams import Behavior, MockUpstream
from gateway.weights import VersionWeighter

log = logging.getLogger(__name__)

S1 = Upstream("stable-v1")
C1 = Upstream("canary-v2")
ROUTE = Route(
    "get-item",
    Method.GET,
    "/items/{item_id}",
    (RouteVersion(1, (S1,)), RouteVersion(2, (C1,))),
)
CONFIGURED = {1: 40, 2: 40}


def make_controller() -> tuple[CanaryController, CircuitBreakerRegistry, ManualClock]:
    clock = ManualClock()
    circuits = CircuitBreakerRegistry(
        CircuitConfig(failure_threshold=2, recovery_seconds=30.0), clock
    )
    canary = CanaryController(
        circuits,
        CanaryConfig(protect_seconds=30.0, step_seconds=10.0, recovery_steps=(0.25, 0.5, 1.0)),
        clock,
    )
    return canary, circuits, clock


def effective(canary: CanaryController) -> dict[int, int]:
    return canary.effective_weights(ROUTE, CONFIGURED)


def run_recovery_sequence() -> list[tuple[str, dict[int, int]]]:
    """完整走一遍 正常->熔断->保护->分阶段恢复，返回每步 (事件, 有效权重) 轨迹。"""
    canary, circuits, clock = make_controller()
    trace: list[tuple[str, dict[int, int]]] = []
    trace.append(("initial", effective(canary)))
    # 连续失败熔断
    circuits.on_failure(C1)
    circuits.on_failure(C1)
    trace.append(("circuit_open", effective(canary)))
    clock.advance(29.0)
    trace.append(("protect_29s", effective(canary)))
    clock.advance(1.0)  # t=30：保护期满，进入恢复 step0
    trace.append(("recover_step0", effective(canary)))
    # 半开探测成功，熔断器闭合；但恢复步进只看时钟，不提前放开
    circuits.before_request(C1)
    circuits.on_success(C1)
    clock.advance(5.0)  # t=35：step0 中途，一个成功结果不得提前提权
    trace.append(("recover_step0_mid", effective(canary)))
    clock.advance(5.0)  # t=40：进入 step1
    trace.append(("recover_step1", effective(canary)))
    clock.advance(10.0)  # t=50：进入 step2（全量）
    trace.append(("recover_step2", effective(canary)))
    clock.advance(10.0)  # t=60：所有阶段完成，回到 NORMAL
    trace.append(("normal", effective(canary)))
    return trace


def test_auto_rollback_and_staged_recovery():
    trace = run_recovery_sequence()
    for event, weights in trace:
        log.info("event=%s -> effective weights=%s", event, weights)
    assert trace[0] == ("initial", {1: 40, 2: 40})
    assert trace[1] == ("circuit_open", {1: 40, 2: 0})       # 自动回退：灰度权重压到 0
    assert trace[2] == ("protect_29s", {1: 40, 2: 0})        # 保护期内不提前放
    assert trace[3] == ("recover_step0", {1: 40, 2: 10})     # 25% 试探
    assert trace[4] == ("recover_step0_mid", {1: 40, 2: 10}) # 迟到的成功不提前放开
    assert trace[5] == ("recover_step1", {1: 40, 2: 20})     # 50%
    assert trace[6] == ("recover_step2", {1: 40, 2: 40})     # 100%
    assert trace[7] == ("normal", {1: 40, 2: 40})


def test_recovery_is_reproducible():
    first = run_recovery_sequence()
    second = run_recovery_sequence()
    log.info("two runs identical=%s", first == second)
    assert first == second


def test_probe_failure_during_recovery_returns_to_protection():
    canary, circuits, clock = make_controller()
    circuits.on_failure(C1)
    circuits.on_failure(C1)
    effective(canary)  # PROTECTED
    clock.advance(30.0)
    weights = effective(canary)  # RECOVERING step0
    log.info("recovering step0 weights=%s", weights)
    assert weights[2] == 10
    # 试探流量触发半开探测，探测失败 -> 熔断器重新 OPEN -> 立即退回保护态
    circuits.before_request(C1)
    circuits.on_failure(C1)
    weights = effective(canary)
    log.info("after failed probe weights=%s state=%s", weights, canary.state_of("get-item", 2))
    assert weights[2] == 0
    assert canary.state_of("get-item", 2) is CanaryState.PROTECTED
    # 重新计满保护期后再次从 step0 开始恢复，不跳步
    clock.advance(30.0)
    weights = effective(canary)
    log.info("re-recovery starts at step0 weights=%s", weights)
    assert weights[2] == 10


def _build_gateway(
    clock: ManualClock, canary_behaviors: list[Behavior]
) -> tuple[Gateway, MockUpstream, MockUpstream, CanaryController]:
    circuits = CircuitBreakerRegistry(
        CircuitConfig(failure_threshold=2, recovery_seconds=30.0), clock
    )
    canary = CanaryController(
        circuits,
        CanaryConfig(protect_seconds=30.0, step_seconds=10.0, recovery_steps=(0.25, 0.5, 1.0)),
        clock,
    )
    weighter = VersionWeighter()
    weighter.update_weights("get-item", CONFIGURED)
    stable = MockUpstream(S1, [Behavior("ok", body={"svc": "v1"})], clock, version=1)
    canary_up = MockUpstream(C1, canary_behaviors, clock, version=2)
    gateway = Gateway(
        Router([ROUTE]),
        RateLimiter(clock),
        circuits,
        {"stable-v1": stable, "canary-v2": canary_up},
        clock=clock,
        weighter=weighter,
        canary=canary,
    )
    return gateway, stable, canary_up, canary


def _request(caller_id: str) -> GatewayRequest:
    return GatewayRequest(Caller(caller_id), Method.GET, "/items/1")


def _find_caller_on_version(gateway: Gateway, version: int) -> str:
    for i in range(200):
        caller = f"caller-{i}"
        if gateway.handle(_request(caller)).version == version:
            log.info("caller %s sticks to version %d", caller, version)
            return caller
    raise AssertionError(f"no caller found on version {version}")


def test_gateway_rolls_traffic_back_to_stable_when_canary_trips():
    clock = ManualClock()
    gateway, stable, canary_up, canary = _build_gateway(clock, [Behavior("ok")])
    caller = _find_caller_on_version(gateway, 2)
    baseline = canary_up.call_count
    # 探测完调用方后再换脚本：连续两次失败 -> 熔断
    canary_up.set_behaviors([Behavior("unavailable"), Behavior("unavailable"), Behavior("ok")])
    for _ in range(2):
        try:
            gateway.handle(_request(caller))
        except Exception as exc:
            log.info("canary call failed as scripted: %s", exc)
    assert canary_up.call_count == baseline + 2
    # 下一请求：灰度版本权重被压到 0，同一调用方落回稳定版本
    resp = gateway.handle(_request(caller))
    log.info(
        "after trip: caller=%s -> version=%d upstream=%s state=%s",
        caller, resp.version, resp.upstream, canary.state_of("get-item", 2),
    )
    assert resp.version == 1
    assert canary.state_of("get-item", 2) is CanaryState.PROTECTED
    assert canary_up.call_count == baseline + 2  # 坏上游不再接收新流量
