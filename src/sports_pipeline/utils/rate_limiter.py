"""Token-bucket rate limiter for API requests."""

from __future__ import annotations

import asyncio
import threading
import time


class TokenBucketRateLimiter:
    """Token bucket rate limiter.

    Allows `rate` requests per `per` seconds with burst capacity equal to rate.
    """

    def __init__(self, rate: int, per: float = 60.0) -> None:
        self.rate = rate
        self.per = per
        self.tokens = float(rate)
        self.max_tokens = float(rate)
        self.last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a token is available."""
        while True:
            with self._lock:
                self._refill()
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait_time = (1.0 - self.tokens) * (self.per / self.rate)
            time.sleep(wait_time)

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.max_tokens, self.tokens + elapsed * (self.rate / self.per))
        self.last_refill = now


class AsyncTokenBucketRateLimiter:
    """Async token bucket rate limiter for use with asyncio.

    Allows `rate` requests per `per` seconds with burst capacity equal to rate.
    """

    def __init__(self, rate: int, per: float = 1.0) -> None:
        self.rate = rate
        self.per = per
        self.tokens = float(rate)
        self.max_tokens = float(rate)
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a token is available."""
        while True:
            async with self._lock:
                self._refill()
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait_time = (1.0 - self.tokens) * (self.per / self.rate)
            await asyncio.sleep(wait_time)

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(
            self.max_tokens, self.tokens + elapsed * (self.rate / self.per)
        )
        self.last_refill = now
