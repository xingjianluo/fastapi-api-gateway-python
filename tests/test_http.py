"""End-to-end HTTP behaviour via FastAPI TestClient."""

from __future__ import annotations

import logging

import pytest

from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.config import (
    CircuitBreakerConfig,
    GatewayConfig,
    RateLimitRule,
    Route,
    UpstreamConfig,
)

log = logging.getLogger("gateway.test.http")


@pytest.fixture
def client():
    cfg = GatewayConfig(
        routes=(
            Route(
                path="/orders/{order_id}",
                methods=frozenset({"GET"}),
                versions=("1.0.0", "2.0.0"),
                upstreams=(
                    UpstreamConfig(name="p", kind="fail"),
                    UpstreamConfig(name="b", kind="ok"),
                ),
                rate_limit=RateLimitRule(capacity=2.0, refill_rate=1.0),
            ),
            Route(
                path="/payments",
                methods=frozenset({"POST"}),
                versions=("1.0.0",),
                upstreams=(
                    UpstreamConfig(name="pp", kind="timeout", delay=9.0),
                    UpstreamConfig(name="pb", kind="ok"),
                ),
            ),
        ),
        circuit_breaker=CircuitBreakerConfig(failure_threshold=3,
                                             cooldown_seconds=30.0),
    )
    with TestClient(create_app(cfg)) as client:
        yield client


def test_success_path_headers(client):
    resp = client.get(
        "/orders/55",
        headers={"X-Caller-Id": "alice", "X-Accept-Version": "2"},
    )
    log.info("GET /orders/55 -> %s headers=%s body=%s",
             resp.status_code,
             {k: v for k, v in resp.headers.items() if k.startswith("X-")},
             resp.json())
    assert resp.status_code == 200
    assert resp.headers["x-served-by"] == "b"
    assert resp.headers["x-api-version"] == "2.0.0"
    assert resp.headers["x-retried"] == "1"


def test_error_classification_headers(client):
    # version failure
    resp = client.get("/orders/1", headers={"X-Accept-Version": "9"})
    log.info("bad version -> %s code=%s",
             resp.status_code, resp.headers.get("x-error-code"))
    assert resp.status_code == 400
    assert resp.headers["x-error-code"] == "version_not_acceptable"
    assert resp.json()["error"]["details"]["available"] == ["1.0.0", "2.0.0"]

    # route not found
    resp = client.get("/nope")
    assert resp.status_code == 404
    assert resp.headers["x-error-code"] == "route_not_found"

    # non-idempotent timeout, not retried
    resp = client.post("/payments", headers={"X-Caller-Id": "x"})
    log.info("POST timeout -> %s code=%s retried=%s",
             resp.status_code, resp.headers["x-error-code"],
             resp.headers["x-retried"])
    assert resp.status_code == 504
    assert resp.headers["x-error-code"] == "upstream_timeout"
    assert resp.headers["x-retried"] == "0"


def test_rate_limit_over_http(client):
    statuses = [
        client.get("/orders/1", headers={"X-Caller-Id": "burst"}).status_code
        for _ in range(3)
    ]
    log.info("burst capacity=2 -> statuses=%s", statuses)
    assert statuses == [200, 200, 429]
    resp = client.get("/orders/1", headers={"X-Caller-Id": "burst"})
    assert resp.json()["error"]["code"] == "rate_limited"


def test_health_endpoint(client):
    assert client.get("/health").json() == {"status": "ok"}
