"""Shared pytest fixtures: deterministic clock, logging, sample configs.

The root logger is configured with ``caplog``-compatible propagation so each
test prints (via ``-s`` / captured logs) the key inputs and the gateway's
decision basis.
"""

from __future__ import annotations

import logging

import pytest

from gateway.clock import FakeClock
from gateway.config import (
    CircuitBreakerConfig,
    GatewayConfig,
    RateLimitRule,
    Route,
    UpstreamConfig,
)


@pytest.fixture(autouse=True)
def _gateway_logs(caplog):
    """Capture gateway decision logs for every test and print them on failure."""
    caplog.set_level(logging.INFO, logger="gateway")
    yield


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(start=1000.0)


@pytest.fixture
def breaker_config() -> CircuitBreakerConfig:
    return CircuitBreakerConfig(
        failure_threshold=3,
        cooldown_seconds=10.0,
        half_open_max_probe=1,
        probe_success_threshold=1,
    )


@pytest.fixture
def failover_config(breaker_config) -> GatewayConfig:
    """Primary fails/times out; backup is healthy. GET route + POST route."""
    return GatewayConfig(
        routes=(
            Route(
                path="/orders/{order_id}",
                methods=frozenset({"GET", "HEAD"}),
                versions=("1.0.0", "2.0.0", "2.1.0"),
                upstreams=(
                    UpstreamConfig(name="orders-primary", kind="fail"),
                    UpstreamConfig(name="orders-backup", kind="ok"),
                ),
                rate_limit=RateLimitRule(
                    capacity=10.0, refill_rate=5.0,
                    quota_limit=50, quota_window=60.0,
                ),
            ),
            Route(
                path="/payments",
                methods=frozenset({"POST"}),
                versions=("1.0.0", "2.0.0"),
                upstreams=(
                    UpstreamConfig(name="payments-primary", kind="timeout",
                                   delay=9.0),
                    UpstreamConfig(name="payments-backup", kind="ok"),
                ),
            ),
        ),
        default_upstreams=(UpstreamConfig(name="default", kind="ok"),),
        circuit_breaker=breaker_config,
    )
