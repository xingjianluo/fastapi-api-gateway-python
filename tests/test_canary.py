"""灰度联动：熔断打开自动压零、分阶段恢复、试探失败退回、迟到成功不提前放开。"""

import logging

from gateway.canary import CanaryConfig, CanaryController, CanaryMode
from gateway.circuit import CircuitState
from gateway.clock import ManualClock

log = logging.getLogger(__name__)

ROUTE, VERSION, UP = "get-item", 2, "svc-b"


def make_controller(ramp=(0.1, 0.5, 1.0), recovery=30.0, dwell=10.0):
    clock = ManualClock()
    controller = CanaryController(
        CanaryConfig(recovery_seconds=recovery, stage_dwell_seconds=dwell, ramp=ramp), clock
    )
    controller.register_version(ROUTE, VERSION, (UP,))
    return controller, clock


def test_open_event_suppresses_weight_immediately():
    controller, _ = make_controller()
    assert controller.effective_weight(ROUTE, VERSION, 100) == 100
    controller.on_circuit_event(UP, CircuitState.OPEN)
    w = controller.effective_weight(ROUTE, VERSION, 100)
    log.info("circuit OPEN -> effective weight %d, mode=%s", w, controller.mode(ROUTE, VERSION))
    assert w == 0
    assert controller.mode(ROUTE, VERSION) is CanaryMode.SUPPRESSED


def test_recovery_waits_full_recovery_seconds():
    controller, clock = make_controller(recovery=30.0)
    controller.on_circuit_event(UP, CircuitState.OPEN)
    clock.advance(29.9)
    w = controller.effective_weight(ROUTE, VERSION, 100)
    log.info("t=29.9 after open -> weight %d", w)
    assert w == 0
    clock.advance(0.1)
    w = controller.effective_weight(ROUTE, VERSION, 100)
    log.info("t=30.0 after open -> weight %d (stage 0 = 10%% of 100)", w)
    assert w == 10


def test_recovery_advances_stage_by_stage_with_dwell():
    controller, clock = make_controller()
    controller.on_circuit_event(UP, CircuitState.OPEN)
    clock.advance(30.0)
    weights = [controller.effective_weight(ROUTE, VERSION, 100)]
    for _ in range(3):
        clock.advance(10.0)
        weights.append(controller.effective_weight(ROUTE, VERSION, 100))
    log.info("stage weights over dwells: %s, final mode=%s", weights, controller.mode(ROUTE, VERSION))
    assert weights == [10, 50, 100, 100]
    assert controller.mode(ROUTE, VERSION) is CanaryMode.NORMAL


def test_partial_dwell_does_not_advance():
    controller, clock = make_controller()
    controller.on_circuit_event(UP, CircuitState.OPEN)
    clock.advance(30.0 + 9.9)
    w = controller.effective_weight(ROUTE, VERSION, 100)
    log.info("recovery + 9.9s (< dwell 10s) -> still stage 0, weight %d", w)
    assert w == 10


def test_open_during_recovery_reverts_to_suppressed():
    controller, clock = make_controller()
    controller.on_circuit_event(UP, CircuitState.OPEN)
    clock.advance(30.0)
    assert controller.effective_weight(ROUTE, VERSION, 100) == 10
    # 试探流量再次打挂上游 -> 立即退回保护
    controller.on_circuit_event(UP, CircuitState.OPEN)
    w = controller.effective_weight(ROUTE, VERSION, 100)
    log.info("probe failed during recovery -> weight %d, mode=%s", w, controller.mode(ROUTE, VERSION))
    assert w == 0
    assert controller.mode(ROUTE, VERSION) is CanaryMode.SUPPRESSED
    # 重新计时：不到 recovery_seconds 不放试探
    clock.advance(29.9)
    assert controller.effective_weight(ROUTE, VERSION, 100) == 0


def test_late_success_does_not_release_early():
    controller, clock = make_controller()
    controller.on_circuit_event(UP, CircuitState.OPEN)
    # 恢复探测尚未到期时，迟到/无关的 CLOSED 事件不得提前放开
    controller.on_circuit_event(UP, CircuitState.CLOSED)
    clock.advance(10.0)
    controller.on_circuit_event(UP, CircuitState.CLOSED)
    w = controller.effective_weight(ROUTE, VERSION, 100)
    log.info("CLOSED events before recovery window -> weight %d", w)
    assert w == 0
    # 进入恢复后，CLOSED 事件也不能代替 dwell 推进
    clock.advance(20.0)
    assert controller.effective_weight(ROUTE, VERSION, 100) == 10
    controller.on_circuit_event(UP, CircuitState.CLOSED)
    w = controller.effective_weight(ROUTE, VERSION, 100)
    log.info("CLOSED event during stage 0 -> weight still %d", w)
    assert w == 10


def test_same_input_sequence_reproduces_same_weights():
    def run():
        controller, clock = make_controller()
        seq = []
        controller.on_circuit_event(UP, CircuitState.OPEN)
        for advance in (10.0, 25.0, 5.0, 10.0, 10.0):
            clock.advance(advance)
            seq.append(controller.effective_weight(ROUTE, VERSION, 100))
        return seq

    first, second = run(), run()
    log.info("deterministic replay: %s vs %s", first, second)
    assert first == second


def test_unrelated_upstream_event_does_not_suppress():
    controller, _ = make_controller()
    controller.on_circuit_event("other-upstream", CircuitState.OPEN)
    w = controller.effective_weight(ROUTE, VERSION, 100)
    log.info("OPEN from unregistered upstream -> weight %d", w)
    assert w == 100


def test_base_weight_zero_stays_zero():
    controller, _ = make_controller()
    w = controller.effective_weight(ROUTE, VERSION, 0)
    log.info("base weight 0 -> effective %d (canary never resurrects)", w)
    assert w == 0
