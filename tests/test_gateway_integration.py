"""Gateway-level integration: config swap atomicity and error normalisation."""

from __future__ import annotations

import logging
import threading

import pytest

from gateway.config import (
    GatewayConfig,
    RateLimitRule,
    Route,
    UpstreamConfig,
)
from gateway.gateway import Gateway, GatewayRequest

log = logging.getLogger("gateway.test.integration")


def _cfg_a() -> GatewayConfig:
    return GatewayConfig(
        routes=(
            Route(
                path="/r",
                methods=frozenset({"GET"}),
                versions=("1.0.0",),
                upstreams=(UpstreamConfig(name="a", kind="ok", body={"v": "A"}),),
                rate_limit=RateLimitRule(capacity=1.0, refill_rate=0.0),
            ),
        ),
    )


def _cfg_b() -> GatewayConfig:
    return GatewayConfig(
        routes=(
            Route(
                path="/r",
                methods=frozenset({"GET"}),
                versions=("2.0.0",),
                upstreams=(UpstreamConfig(name="b", kind="ok", body={"v": "B"}),),
                rate_limit=RateLimitRule(capacity=100.0, refill_rate=100.0),
            ),
        ),
    )


def test_atomic_config_swap(clock):
    gw = Gateway(_cfg_a(), clock=clock)
    r1 = gw.handle(GatewayRequest("GET", "/r"))
    assert r1.version == "1.0.0" and r1.upstream == "a"

    gw.update_config(_cfg_b())
    r2 = gw.handle(GatewayRequest("GET", "/r"))
    log.info("after swap -> version=%s upstream=%s body=%s",
             r2.version, r2.upstream, r2.body)
    assert r2.version == "2.0.0"
    assert r2.upstream == "b"
    # Old config's exhausted bucket must not leak into the new rule:
    # config A had capacity 1 and used it; B allows this request fine.
    assert r2.status == 200


def test_no_mixed_config_under_concurrent_swap(clock):
    """During many swaps every response is entirely-A or entirely-B."""
    gw = Gateway(_cfg_a(), clock=clock)
    observations: set[tuple] = set()
    lock = threading.Lock()
    stop = threading.Event()

    def traffic() -> None:
        while not stop.is_set():
            resp = gw.handle(GatewayRequest("GET", "/r",
                                            version_header="1.0.0",
                                            caller=str(
                                                threading.get_ident() % 50)))
            if resp.status == 200:
                with lock:
                    observations.add((resp.version, resp.upstream))
            # Rejections (rate_limited from config A) are also legitimate,
            # but never a mixture like version 1 served by upstream b.

    def swapper() -> None:
        for _ in range(100):
            gw.update_config(_cfg_b())
            gw.update_config(_cfg_a())

    t1 = threading.Thread(target=traffic)
    t2 = threading.Thread(target=swapper)
    t1.start(); t2.start(); t2.join()
    stop.set(); t1.join()
    log.info("successful (version,upstream) pairs observed: %s", observations)
    assert observations <= {("1.0.0", "a"), ("2.0.0", "b")}


def test_handle_never_raises(clock):
    gw = Gateway(_cfg_a(), clock=clock)
    resp = gw.handle(GatewayRequest("GET", "/does/not/exist"))
    assert 400 <= resp.status < 600
    assert resp.error_code == "route_not_found"
    assert isinstance(resp.body, dict) and "error" in resp.body


@pytest.mark.parametrize(
    "kind,delay,expected_code,expected_status",
    [
        ("fail", 0.0, "upstream_unavailable", 502),
        ("timeout", 9.0, "upstream_timeout", 504),
        ("busy", 0.0, "upstream_business_error", 502),
    ],
)
def test_error_types_are_normalised_and_distinct(
    clock, kind, delay, expected_code, expected_status
):
    cfg = GatewayConfig(
        routes=(
            Route(
                path="/r",
                methods=frozenset({"GET"}),
                versions=("1.0.0",),
                upstreams=(UpstreamConfig(name="only", kind=kind, delay=delay),),
            ),
        ),
    )
    gw = Gateway(cfg, clock=clock)
    resp = gw.handle(GatewayRequest("GET", "/r", timeout=1.0))
    log.info("kind=%s -> code=%s status=%s (expected %s/%s)",
             kind, resp.error_code, resp.status, expected_code, expected_status)
    assert resp.error_code == expected_code
    assert resp.status == expected_status
    # Body uses the public normalised envelope, no raw exception text leaking.
    assert resp.body["error"]["code"] == expected_code
    assert "traceback" not in str(resp.body).lower()
