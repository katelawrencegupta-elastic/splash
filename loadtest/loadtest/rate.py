"""Token-bucket rate limiter (events/s)."""

from __future__ import annotations

import asyncio
import time


class RateLimiter:
    """Async token bucket. ``rate`` is permits per second; 0 = unlimited."""

    def __init__(self, rate: float, *, burst: float | None = None) -> None:
        self.rate = max(0.0, float(rate))
        self.burst = float(burst) if burst is not None else max(self.rate, 1.0)
        self._tokens = self.burst
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    def set_rate(self, rate: float) -> None:
        self.rate = max(0.0, float(rate))
        self.burst = max(self.rate, 1.0)

    async def acquire(self, n: float = 1.0) -> None:
        if self.rate <= 0:
            return
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._updated
                self._updated = now
                self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
                if self._tokens >= n:
                    self._tokens -= n
                    return
                need = n - self._tokens
                wait = need / self.rate
            await asyncio.sleep(wait)
