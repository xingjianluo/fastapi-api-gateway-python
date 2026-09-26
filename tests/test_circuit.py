import logging

import pytest

from gateway.circuit import CircuitBreakerRegistry, CircuitConfig, CircuitState
from gateway.clock import ManualClock
from gateway.errors import ErrorCategory, GatewayError
from gateway.models import Upstream

log = logging.getLogger(__name__)

UP = Upstream("svc-a")


def make_registry(threshold: int = 3, recovery: float = 30.0, probes: int = 1):
    clock = ManualClock()
    registry = CircuitBreakerRegistry(
        CircuitConfig(threshold, recovery, probes), clock
    )
    return registry, clock


def test_opens_after_consecutive_failures():
    registry, _ = make_registry(threshold=3)
    for i in range(3):
        registry.before_request(UP)
        registry.on_failure(UP)
        log.info("failure #%d -> state=%s", i + 1, registry.state(UP).value)
    assert registry.state(UP) is CircuitState.OPEN
    with pytest.raises(GatewayError) as exc:
        registry.before_request(UP)
    assert exc.value.category is ErrorCategory.CIRCUIT_OPEN


def test_success_resets_failure_count():
    registry, _ = make_registry(threshold=3)
    registry.before_request(UP)
    registry.on_failure(UP)
    registry.before_request(UP)
    registry.on_failure(UP)
    registry.before_request(UP)
    registry.on_success(UP)
    registry.before_request(UP)
    registry.on_failure(UP)
    log.info("2 fail, success, 1 fail -> state=%s", registry.state(UP).value)
    assert registry.state(UP) is CircuitState.CLOSED


def test_half_open_after_recovery_and_probe_success_closes():
    registry, clock = make_registry(threshold=2, recovery=30.0)
    registry.before_request(UP)
    registry.on_failure(UP)
    registry.before_request(UP)
    registry.on_failure(UP)
    assert registry.state(UP) is CircuitState.OPEN
    clock.advance(29.9)
    with pytest.raises(GatewayError):
        registry.before_request(UP)
    clock.advance(0.1)
    registry.before_request(UP)  # 半开探测放行
    log.info("after 30s -> state=%s", registry.state(UP).value)
    assert registry.state(UP) is CircuitState.HALF_OPEN
    registry.on_success(UP)
    assert registry.state(UP) is CircuitState.CLOSED


def test_half_open_probe_failure_reopens():
    registry, clock = make_registry(threshold=2, recovery=30.0)
    registry.before_request(UP)
    registry.on_failure(UP)
    registry.before_request(UP)
    registry.on_failure(UP)
    clock.advance(30.0)
    registry.before_request(UP)
    registry.on_failure(UP)  # 探测失败
    log.info("probe failed -> state=%s", registry.state(UP).value)
    assert registry.state(UP) is CircuitState.OPEN
    with pytest.raises(GatewayError):
        registry.before_request(UP)
    clock.advance(30.0)
    registry.before_request(UP)  # 重新计时后再次允许探测


def test_half_open_limits_concurrent_probes():
    registry, clock = make_registry(threshold=1, recovery=10.0, probes=1)
    registry.before_request(UP)
    registry.on_failure(UP)
    clock.advance(10.0)
    registry.before_request(UP)  # 占用唯一探测名额
    with pytest.raises(GatewayError) as exc:
        registry.before_request(UP)
    log.info("second concurrent probe -> %s", exc.value.category)
    assert exc.value.category is ErrorCategory.CIRCUIT_OPEN
