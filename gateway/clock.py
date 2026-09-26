"""Controllable clock abstraction so time-related behaviour is deterministic in tests.

Real code uses :class:`SystemClock`; tests use :class:`FakeClock`, whose time
only moves via :meth:`FakeClock.advance`, making cooldown windows, refill and
timeout behaviour fully reproducible.
"""

from __future__ import annotations

import threading
import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """Return current monotonic-ish time in seconds."""
        ...

    def sleep(self, seconds: float) -> None:
        """Advance / wait for the given amount of time."""
        ...


class SystemClock:
    """Real wall-clock implementation backed by :func:`time.monotonic`."""

    def now(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(max(0.0, seconds))


class FakeClock:
    """Deterministic, thread-safe clock that only moves when :meth:`advance` is called.

    Threads blocked in :meth:`sleep` are woken when enough simulated time has
    elapsed, so concurrency tests still exercise real thread scheduling while
    the observed timeline is fully controlled by the test.
    """

    def __init__(self, start: float = 0.0) -> None:
        self._now = float(start)
        self._cond = threading.Condition()

    def now(self) -> float:
        with self._cond:
            return self._now

    def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            # Still yield control so other threads can progress.
            with self._cond:
                self._cond.wait(0)
            return
        deadline = self.now() + seconds
        with self._cond:
            while self._now < deadline:
                remaining = deadline - self._now
                self._cond.wait(timeout=min(remaining, 0.01))

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("time cannot go backwards")
        with self._cond:
            self._now += seconds
            self._cond.notify_all()
