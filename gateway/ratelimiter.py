"""Atomic per-(caller, route) rate limiting and quota control.

Two independent controls are applied to every request, and each produces a
distinguishable rejection reason:

* **Token bucket** — smooth burst control at ``refill_rate`` tokens/second up
  to ``capacity``.  A request whose cost exceeds ``capacity`` can never fit and
  is rejected as ``scope_insufficient`` (different from a temporary burst).
* **Window quota** — at most ``quota_limit`` accepted requests per
  ``quota_window`` seconds; exhaustion yields ``quota_exhausted``.

All state transitions happen under a single lock per key, so concurrent
counters are atomic.  :meth:`RateLimiter.update_rules` swaps the whole rule
table in one assignment; an in-flight request keeps using the snapshot it
started with, so old and new configuration can never be mixed mid-request.
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass

from .clock import Clock
from .config import RateLimitRule
from .errors import (
    QuotaExhaustedError,
    RateLimitedError,
    ScopeInsufficientError,
)


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    remaining_tokens: float
    remaining_quota: int | None
    rule: RateLimitRule | None
    retry_after: float = 0.0


@dataclass
class _BucketState:
    tokens: float
    last_refill: float
    quota_remaining: int | None
    window_start: float


class RateLimiter:
    """Token-bucket burst control + windowed quota, keyed by caller and route."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._rules: dict[str, RateLimitRule | None] = {}
        self._states: dict[tuple[str, str], _BucketState] = {}
        self._lock = threading.RLock()

    def update_rules(self, rules: dict[str, RateLimitRule | None]) -> None:
        """Atomically swap the whole rule table; no mixed old/new state.

        Stale per-key counters are dropped; the first request under the new
        rule starts from a fresh, fully-charged bucket.
        """
        with self._lock:
            self._rules = dict(rules)
            self._states.clear()

    def check(
        self,
        caller: str,
        route_key: str,
        rule: RateLimitRule | None,
        cost: float = 1.0,
    ) -> RateLimitDecision:
        if rule is None:
            return RateLimitDecision(
                allowed=True,
                remaining_tokens=float("inf"),
                remaining_quota=None,
                rule=None,
            )
        if cost < 0:
            raise ValueError("cost must be non-negative")

        with self._lock:
            now = self._clock.now()
            key = (caller, route_key)
            state = self._states.get(key)
            if state is None:
                state = _BucketState(
                    tokens=rule.capacity,
                    last_refill=now,
                    quota_remaining=rule.quota_limit,
                    window_start=now,
                )
                self._states[key] = state

            # Refill bucket based on elapsed time.
            elapsed = now - state.last_refill
            if elapsed > 0:
                state.tokens = min(
                    rule.capacity, state.tokens + elapsed * rule.refill_rate
                )
                state.last_refill = now

            # Rolling quota window.
            if rule.quota_limit is not None:
                if now - state.window_start >= rule.quota_window:
                    state.window_start = now
                    state.quota_remaining = rule.quota_limit

            # 1) Single request larger than the bucket can ever be.
            if cost > rule.capacity:
                raise ScopeInsufficientError(
                    f"request cost {cost:g} exceeds bucket capacity "
                    f"{rule.capacity:g}",
                    details={
                        "caller": caller,
                        "route": route_key,
                        "cost": cost,
                        "capacity": rule.capacity,
                    },
                )

            # 2) Quota exhausted (distinct from a momentary token shortage).
            if (
                rule.quota_limit is not None
                and state.quota_remaining is not None
                and state.quota_remaining < max(1, math.ceil(cost))
            ):
                reset_in = max(0.0, state.window_start + rule.quota_window - now)
                raise QuotaExhaustedError(
                    f"quota {rule.quota_limit} per {rule.quota_window:g}s exhausted",
                    details={
                        "caller": caller,
                        "route": route_key,
                        "quota_limit": rule.quota_limit,
                        "quota_remaining": state.quota_remaining,
                        "reset_in_seconds": round(reset_in, 6),
                    },
                )

            # 3) Not enough tokens right now -> temporary burst rejection.
            if state.tokens + 1e-9 < cost:
                retry_after = (cost - state.tokens) / max(rule.refill_rate, 1e-12)
                raise RateLimitedError(
                    f"rate limit exceeded: need {cost:g} tokens, "
                    f"have {state.tokens:.3f}",
                    details={
                        "caller": caller,
                        "route": route_key,
                        "tokens": round(state.tokens, 6),
                        "capacity": rule.capacity,
                        "refill_rate": rule.refill_rate,
                        "retry_after_seconds": round(retry_after, 6),
                    },
                )

            state.tokens -= cost
            if rule.quota_limit is not None and state.quota_remaining is not None:
                state.quota_remaining -= max(1, math.ceil(cost))

            return RateLimitDecision(
                allowed=True,
                remaining_tokens=state.tokens,
                remaining_quota=state.quota_remaining,
                rule=rule,
            )
