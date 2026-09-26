"""可控时钟抽象，时间相关行为均可通过 ManualClock 推进。"""

from __future__ import annotations

import threading
import time
from typing import Protocol


class Clock(Protocol):
    """单调时钟，返回秒。"""

    def now(self) -> float: ...


class SystemClock:
    def now(self) -> float:
        return time.monotonic()


class ManualClock:
    """测试用可控时钟，线程安全。"""

    def __init__(self, start: float = 0.0) -> None:
        self._now = start
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> float:
        if seconds < 0:
            raise ValueError("cannot move clock backwards")
        with self._lock:
            self._now += seconds
            return self._now
