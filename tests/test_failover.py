"""Failover semantics: idempotent retry vs. non-idempotent single execution."""

from __future__ import annotations

import logging

import pytest

from gateway.config import (
    CircuitBreakerConfig,
    GatewayConfig,
    Route,
    UpstreamConfig,
)
from gateway.gateway import Gateway, GatewayRequest

log = logging.getLogger("gateway.test.failover")


def make_gateway(clock, *, primary_kind="fail", primary_delay=0.0,
                 backup_kind="ok", sequence=(), methods=("GET", "POST"),
                 failure_threshold=3):
    if sequence:
        primary_kind = "script"
    return Gateway(
        GatewayConfig(
            routes=(
                Route(
                    path="/resource",
                    methods=frozenset(methods),
                    versions=("1.0.0",),
                    upstreams=(
                        UpstreamConfig(name="primary", kind=primary_kind,
                                       delay=primary_delay, sequence=sequence),
                        UpstreamConfig(name="backup", kind=backup_kind),
                    ),
                ),
            ),
            circuit_breaker=CircuitBreakerConfig(
                failure_threshold=failure_threshold,
                cooldown_seconds=10.0,
            ),
        ),
        clock=clock,
    )


def test_idempotent_get_fails_over_to_backup(clock, failover_config):
    gw = Gateway(failover_config, clock=clock)
    resp = gw.handle(GatewayRequest(
        "GET", "/orders/42", caller="alice", version_header="2"
    ))
    log.info("GET primary=fail -> status=%s served_by=%s retried=%s",
             resp.status, resp.upstream, resp.retried)
    assert resp.status == 200
    assert resp.upstream == "orders-backup"
    assert resp.retried is True
    assert gw.pool.get(failover_config.routes[0].upstreams[0]).call_count == 1
    assert gw.pool.get(failover_config.routes[0].upstreams[1]).call_count == 1


def test_non_idempotent_post_is_never_replayed(clock, failover_config):
    gw = Gateway(failover_config, clock=clock)
    primary = gw.pool.get(failover_config.routes[1].upstreams[0])
    backup = gw.pool.get(failover_config.routes[1].upstreams[1])

    resp = gw.handle(GatewayRequest(
        "POST", "/payments", caller="alice", timeout=5.0
    ))
    log.info("POST primary=timeout -> status=%s code=%s retried=%s | "
             "primary_calls=%d backup_calls=%d",
             resp.status, resp.error_code, resp.retried,
             primary.call_count, backup.call_count)
    assert resp.status == 504
    assert resp.error_code == "upstream_timeout"
    assert resp.retried is False
    assert primary.call_count == 1
    assert backup.call_count == 0  # payment must never be executed twice


def test_timeout_on_idempotent_route_also_fails_over(clock):
    gw = make_gateway(clock, primary_kind="timeout", primary_delay=9.0,
                      methods=("GET",))
    resp = gw.handle(GatewayRequest("GET", "/resource", timeout=5.0))
    log.info("idempotent timeout -> status=%s upstream=%s retried=%s",
             resp.status, resp.upstream, resp.retried)
    assert resp.status == 200
    assert resp.upstream == "backup"
    assert resp.retried is True


def test_business_error_is_not_retried_even_for_idempotent(clock):
    gw = make_gateway(clock, primary_kind="busy", methods=("GET",))
    primary = gw.pool.get(gw._config.routes[0].upstreams[0])
    backup = gw.pool.get(gw._config.routes[0].upstreams[1])
    resp = gw.handle(GatewayRequest("GET", "/resource"))
    log.info("idempotent business error -> code=%s primary_calls=%d "
             "backup_calls=%d", resp.error_code, primary.call_count,
             backup.call_count)
    assert resp.error_code == "upstream_business_error"
    assert primary.call_count == 1
    assert backup.call_count == 0


def test_all_upstreams_unavailable_reports_last_reason(clock):
    gw = make_gateway(clock, primary_kind="fail", backup_kind="fail",
                      methods=("GET",))
    resp = gw.handle(GatewayRequest("GET", "/resource"))
    log.info("both fail -> status=%s code=%s", resp.status, resp.error_code)
    assert resp.status == 502
    assert resp.error_code == "upstream_unavailable"


def test_failover_does_not_duplicate_on_eventual_primary_recovery(clock):
    """Scripted primary fails first then recovers; first call hits backup,
    second call is served by recovered primary."""
    gw = make_gateway(clock, sequence=("fail", "ok"), methods=("GET",),
                      failure_threshold=10_000)
    r1 = gw.handle(GatewayRequest("GET", "/resource"))
    r2 = gw.handle(GatewayRequest("GET", "/resource"))
    log.info("scripted fail,ok -> r1=%s/%s r2=%s/%s",
             r1.upstream, r1.retried, r2.upstream, r2.retried)
    assert r1.upstream == "backup" and r1.retried is True
    assert r2.upstream == "primary" and r2.retried is False
