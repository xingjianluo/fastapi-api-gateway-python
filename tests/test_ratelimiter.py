"""Atomic rate limiting: burst, quota, scope, refill and atomic rule swaps."""

from __future__ import annotations

import logging
import threading

import pytest

from gateway.clock import FakeClock
from gateway.config import RateLimitRule
from gateway.errors import (
    QuotaExhaustedError,
    RateLimitedError,
    ScopeInsufficientError,
)
from gateway.ratelimiter import RateLimiter

log = logging.getLogger("gateway.test.ratelimit")


def rule(capacity=10.0, refill=5.0, quota=None, window=60.0) -> RateLimitRule:
    return RateLimitRule(
        capacity=capacity, refill_rate=refill,
        quota_limit=quota, quota_window=window,
    )


def test_concurrent_burst_counter_is_atomic(clock):
    """20 threads x 1 request against capacity 10 -> exactly 10 allowed."""
    limiter = RateLimiter(clock)
    rl = rule(capacity=10.0, refill=0.0)
    results: list[bool] = []
    lock = threading.Lock()
    barrier = threading.Barrier(20)

    def worker() -> None:
        barrier.wait()  # maximise contention
        try:
            limiter.check("c1", "/r", rl, cost=1.0)
            allowed = True
        except RateLimitedError:
            allowed = False
        with lock:
            results.append(allowed)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    allowed = sum(results)
    log.info("concurrent burst input=20 threads, capacity=10 -> allowed=%d "
             "rejected=%d (must be 10/10)", allowed, len(results) - allowed)
    assert len(results) == 20
    assert allowed == 10
    assert results.count(False) == 10


def test_burst_then_refill(clock):
    limiter = RateLimiter(clock)
    rl = rule(capacity=4.0, refill=2.0)
    for _ in range(4):
        assert limiter.check("c", "/r", rl).allowed
    with pytest.raises(RateLimitedError) as exc:
        limiter.check("c", "/r", rl)
    log.info("burst exhausted -> %s retry_after=%s",
             exc.value.code.value, exc.value.details["retry_after_seconds"])
    assert exc.value.code.value == "rate_limited"

    clock.advance(0.5)  # one token refilled
    decision = limiter.check("c", "/r", rl)
    log.info("after +0.5s at refill 2/s -> allowed tokens_left=%.2f",
             decision.remaining_tokens)
    assert decision.allowed
    with pytest.raises(RateLimitedError):
        limiter.check("c", "/r", rl)


def test_request_larger_than_capacity_is_scope_insufficient(clock):
    limiter = RateLimiter(clock)
    rl = rule(capacity=5.0, refill=5.0)
    with pytest.raises(ScopeInsufficientError) as exc:
        limiter.check("c", "/r", rl, cost=6.0)
    log.info("oversize request cost=6 capacity=5 -> %s", exc.value.code.value)
    assert exc.value.code.value == "scope_insufficient"
    # A scope rejection must not consume tokens: next normal request succeeds.
    assert limiter.check("c", "/r", rl, cost=1.0).allowed


def test_quota_exhaustion_is_distinct_from_burst(clock):
    limiter = RateLimiter(clock)
    rl = rule(capacity=100.0, refill=1000.0, quota=3, window=10.0)
    for i in range(3):
        limiter.check("c", "/r", rl)
    with pytest.raises(QuotaExhaustedError) as exc:
        limiter.check("c", "/r", rl)
    err = exc.value
    log.info("quota=3/10s exhausted -> code=%s reset_in=%s",
             err.code.value, err.details["reset_in_seconds"])
    assert err.code.value == "quota_exhausted"

    clock.advance(10.0)
    decision = limiter.check("c", "/r", rl)
    log.info("quota window reset -> allowed=%s remaining_quota=%s",
             decision.allowed, decision.remaining_quota)
    assert decision.allowed
    assert decision.remaining_quota == 2


def test_per_caller_and_route_keys_are_independent(clock):
    limiter = RateLimiter(clock)
    rl = rule(capacity=1.0, refill=0.0)
    assert limiter.check("alice", "/a", rl).allowed
    assert limiter.check("bob", "/a", rl).allowed
    assert limiter.check("alice", "/b", rl).allowed
    with pytest.raises(RateLimitedError):
        limiter.check("alice", "/a", rl)
    log.info("per-(caller,route) buckets verified independent")


def test_rule_swap_has_no_mixed_state(clock):
    limiter = RateLimiter(clock)
    old = rule(capacity=1.0, refill=0.0)
    limiter.update_rules({"/r": old})
    limiter.check("c", "/r", old)  # exhaust old bucket
    with pytest.raises(RateLimitedError):
        limiter.check("c", "/r", old)

    new = rule(capacity=10.0, refill=1.0)
    limiter.update_rules({"/r": new})  # atomic swap resets counters
    decision = limiter.check("c", "/r", new)
    log.info("rule swap capacity 1->10: first post-swap tokens_left=%.2f "
             "(fresh bucket expected)", decision.remaining_tokens)
    assert decision.allowed
    assert decision.remaining_tokens == 9.0


def test_concurrent_rule_swap_during_traffic(clock):
    """While rules swap, every decision must match exactly one rule version."""
    limiter = RateLimiter(clock)
    limiter.update_rules({"/r": rule(capacity=4.0, refill=100.0)})
    seen_capacities: set[float] = set()
    lock = threading.Lock()

    def traffic() -> None:
        for _ in range(200):
            rl = limiter._rules.get("/r")  # snapshot observed by traffic
            try:
                dec = limiter.check("c", "/r", rl, cost=1.0)
                cap = dec.rule.capacity if dec.rule else -1
            except RateLimitedError as e:
                cap = e.details["capacity"]
            with lock:
                seen_capacities.add(cap)

    def swapper() -> None:
        for i in range(20):
            cap = 4.0 if i % 2 == 0 else 8.0
            limiter.update_rules({"/r": rule(capacity=cap, refill=100.0)})

    t1 = threading.Thread(target=traffic)
    t2 = threading.Thread(target=swapper)
    t1.start(); t2.start(); t1.join(); t2.join()
    log.info("capacities observed during swaps: %s (subset of {4,8} expected)",
             sorted(seen_capacities))
    assert seen_capacities <= {4.0, 8.0}
