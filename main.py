"""Local entry point: build the demo config and serve the gateway."""

from __future__ import annotations

import logging

import uvicorn

from gateway.app import create_app
from gateway.config import (
    CircuitBreakerConfig,
    GatewayConfig,
    RateLimitRule,
    Route,
    UpstreamConfig,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)


def build_demo_config() -> GatewayConfig:
    """Routes used by ``python main.py`` and the end-to-end HTTP demo."""
    orders_route = Route(
        path="/orders/{order_id}",
        methods=frozenset({"GET"}),
        versions=("1.0.0", "2.0.0"),
        upstreams=(
            UpstreamConfig(name="orders-primary", kind="fail"),
            UpstreamConfig(name="orders-backup", kind="ok"),
        ),
        rate_limit=RateLimitRule(
            capacity=5.0, refill_rate=1.0, quota_limit=20, quota_window=60.0
        ),
    )
    flaky_route = Route(
        path="/payments",
        methods=frozenset({"POST", "GET"}),
        versions=("1.0.0",),
        upstreams=(
            # POST is non-idempotent: the timeout below is never retried.
            UpstreamConfig(name="payments-primary", kind="timeout", delay=9.0),
            UpstreamConfig(name="payments-backup", kind="ok"),
        ),
        rate_limit=RateLimitRule(capacity=100.0, refill_rate=10.0),
    )
    return GatewayConfig(
        routes=(orders_route, flaky_route),
        default_upstreams=(UpstreamConfig(name="default-primary", kind="ok"),),
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=3,
            cooldown_seconds=30.0,
            half_open_max_probe=1,
            probe_success_threshold=1,
        ),
    )


app = create_app(build_demo_config())


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
