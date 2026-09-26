import logging
import threading

import pytest

from gateway.clock import ManualClock
from gateway.errors import ErrorCategory, GatewayError
from gateway.ratelimit import RateLimiter, RateLimitRule

log = logging.getLogger(__name__)

KEY = ("caller-1", "route-a")


def make_limiter(burst: int, window: float, quota: int) -> tuple[RateLimiter, ManualClock]:
    clock = ManualClock()
    limiter = RateLimiter(clock)
    limiter.update_rules({KEY: RateLimitRule(burst, window, quota)})
    return limiter, clock


def test_burst_limit_rejects_with_rate_limited():
    limiter, _ = make_limiter(burst=2, window=10.0, quota=100)
    limiter.check(*KEY)
    limiter.check(*KEY)
    with pytest.raises(GatewayError) as exc:
        limiter.check(*KEY)
    log.info("burst=2, 3rd request -> %s", exc.value.category)
    assert exc.value.category is ErrorCategory.RATE_LIMITED


def test_window_rollover_allows_again():
    limiter, clock = make_limiter(burst=1, window=10.0, quota=100)
    limiter.check(*KEY)
    clock.advance(10.0)
    limiter.check(*KEY)  # 新窗口放行
    log.info("advanced clock past window -> allowed again")


def test_quota_exhaustion_is_distinct_from_burst():
    limiter, clock = make_limiter(burst=1, window=10.0, quota=2)
    limiter.check(*KEY)
    clock.advance(10.0)
    limiter.check(*KEY)
    clock.advance(10.0)
    with pytest.raises(GatewayError) as exc:
        limiter.check(*KEY)
    log.info("quota=2, 3rd window request -> %s", exc.value.category)
    assert exc.value.category is ErrorCategory.QUOTA_EXHAUSTED


def test_concurrent_checks_never_exceed_burst():
    limiter, _ = make_limiter(burst=50, window=60.0, quota=10_000)
    allowed = 0
    lock = threading.Lock()

    def worker() -> None:
        nonlocal allowed
        for _ in range(20):
            try:
                limiter.check(*KEY)
                with lock:
                    allowed += 1
            except GatewayError:
                pass

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log.info("burst=50, 10 threads x 20 reqs -> allowed=%d", allowed)
    assert allowed == 50


def test_rule_update_is_atomic_snapshot():
    limiter, _ = make_limiter(burst=1, window=10.0, quota=100)
    limiter.check(*KEY)
    # 替换为新规则：新快照立即整体生效
    limiter.update_rules({KEY: RateLimitRule(burst_limit=5, window_seconds=10.0, quota=100)})
    for _ in range(4):
        limiter.check(*KEY)
    with pytest.raises(GatewayError) as exc:
        limiter.check(*KEY)
    log.info("after snapshot swap burst=5, 6th request -> %s", exc.value.category)
    assert exc.value.category is ErrorCategory.RATE_LIMITED


def test_no_rule_allows():
    limiter = RateLimiter(ManualClock())
    limiter.check("nobody", "nothing")
