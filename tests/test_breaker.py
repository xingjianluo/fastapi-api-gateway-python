"""Circuit-breaker state migration driven by a controllable clock."""

from __future__ import annotations

import logging
import threading

import pytest

from gateway.breaker import BreakerState, CircuitBreakerRegistry
from gateway.clock import FakeClock
from gateway.config import CircuitBreakerConfig
from gateway.errors import CircuitOpenError

log = logging.getLogger("gateway.test.breaker")


def make_registry(clock, *, threshold=3, cooldown=10.0, max_probe=1,
                  success_threshold=1):
    return CircuitBreakerRegistry(
        CircuitBreakerConfig(
            failure_threshold=threshold,
            cooldown_seconds=cooldown,
            half_open_max_probe=max_probe,
            probe_success_threshold=success_threshold,
        ),
        clock,
    )


def test_closed_to_open_after_threshold(clock):
    reg = make_registry(clock)
    states = []
    for i in range(4):
        snap = reg.record_failure("u")
        states.append((i + 1, snap.state.value, snap.consecutive_failures))
    log.info("failure sequence -> %s (expect open at #3)", states)
    assert reg.snapshot("u").state is BreakerState.OPEN

    with pytest.raises(CircuitOpenError) as exc:
        reg.allow_request("u")
    log.info("open breaker rejects -> %s retry_after=%s",
             exc.value.code.value, exc.value.details["retry_after_seconds"])
    assert exc.value.code.value == "upstream_circuit_open"


def test_open_halfopen_closed_on_successful_probe(clock):
    reg = make_registry(clock)
    for _ in range(3):
        reg.record_failure("u")
    assert reg.snapshot("u").state is BreakerState.OPEN

    clock.advance(10.0)  # cooldown elapsed
    snap = reg.allow_request("u")
    log.info("after cooldown allow_request -> state=%s probes=%d",
             snap.state.value, snap.probes_in_flight)
    assert snap.state is BreakerState.HALF_OPEN
    assert snap.probes_in_flight == 1

    snap = reg.record_success("u")
    log.info("successful probe -> state=%s failures=%d",
             snap.state.value, snap.consecutive_failures)
    assert snap.state is BreakerState.CLOSED
    assert snap.consecutive_failures == 0
    # Normal traffic flows again.
    assert reg.allow_request("u").state is BreakerState.CLOSED


def test_failed_probe_reopens_immediately(clock):
    reg = make_registry(clock)
    for _ in range(3):
        reg.record_failure("u")
    clock.advance(10.0)
    assert reg.allow_request("u").state is BreakerState.HALF_OPEN
    snap = reg.record_failure("u")
    log.info("failed probe -> state=%s (reopened)", snap.state.value)
    assert snap.state is BreakerState.OPEN

    # Cooldown restarts: still rejected before another 10s.
    clock.advance(5.0)
    with pytest.raises(CircuitOpenError):
        reg.allow_request("u")
    clock.advance(5.0)
    assert reg.allow_request("u").state is BreakerState.HALF_OPEN


def test_half_open_limits_concurrent_probes(clock):
    """Only half_open_max_probe requests may probe; others stay rejected."""
    reg = make_registry(clock, max_probe=2)
    for _ in range(3):
        reg.record_failure("u")
    clock.advance(10.0)

    admitted = []
    rejected = []
    lock = threading.Lock()
    barrier = threading.Barrier(5)

    def attempt() -> None:
        barrier.wait()
        try:
            reg.allow_request("u")
            with lock:
                admitted.append(True)
        except CircuitOpenError:
            with lock:
                rejected.append(True)

    threads = [threading.Thread(target=attempt) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    log.info("half-open 5 concurrent attempts -> admitted=%d rejected=%d "
             "(max_probe=2)", len(admitted), len(rejected))
    assert len(admitted) == 2
    assert len(rejected) == 3


def test_probe_success_threshold_requires_stable_judgement(clock):
    reg = make_registry(clock, success_threshold=2)
    for _ in range(3):
        reg.record_failure("u")
    clock.advance(10.0)

    reg.allow_request("u")
    snap = reg.record_success("u")
    log.info("1/2 probe successes -> state=%s", snap.state.value)
    assert snap.state is BreakerState.HALF_OPEN

    # A new probe must be admitted in place of the completed one.
    reg.allow_request("u")
    snap = reg.record_success("u")
    log.info("2/2 probe successes -> state=%s", snap.state.value)
    assert snap.state is BreakerState.CLOSED


def test_gateway_level_breaker_then_recovery(clock, failover_config):
    """Only-upstream route: failures open the breaker; recovery via probes."""
    from gateway.config import GatewayConfig, Route, UpstreamConfig
    from gateway.gateway import Gateway, GatewayRequest

    cfg = GatewayConfig(
        routes=(
            Route(
                path="/solo",
                methods=frozenset({"GET"}),
                versions=("1.0.0",),
                upstreams=(
                    UpstreamConfig(name="solo-u", kind="script",
                                   sequence=("fail", "fail", "fail", "ok")),
                ),
            ),
        ),
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=3, cooldown_seconds=10.0,
            half_open_max_probe=1, probe_success_threshold=1,
        ),
    )
    gw = Gateway(cfg, clock=clock)
    codes = [gw.handle(GatewayRequest("GET", "/solo")).error_code for _ in range(5)]
    log.info("responses while tripping: %s", codes)
    assert codes[:3] == ["upstream_unavailable"] * 3
    assert codes[3:] == ["upstream_circuit_open"] * 2

    clock.advance(10.0)
    resp = gw.handle(GatewayRequest("GET", "/solo"))
    log.info("probe after cooldown -> status=%s breaker=%s",
             resp.status, gw.breaker.snapshot("solo-u").state.value)
    assert resp.status == 200
    assert gw.breaker.snapshot("solo-u").state is BreakerState.CLOSED
