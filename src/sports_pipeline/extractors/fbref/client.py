"""FBref HTTP client with rate limiting and caching."""

from __future__ import annotations

import hashlib
from pathlib import Path

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from sports_pipeline.config import PROJECT_ROOT, get_settings
from sports_pipeline.utils.logging import get_logger
from sports_pipeline.utils.rate_limiter import TokenBucketRateLimiter

log = get_logger(__name__)


class FbrefClient:
    """HTTP client for FBref with rate limiting."""

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (compatible; sports-pipeline/0.1)"
    }

    def __init__(self, cache_dir: Path | None = None) -> None:
        settings = get_settings()
        rpm = settings.rate_limits.fbref.requests_per_minute
        self._limiter = TokenBucketRateLimiter(rate=rpm, per=60.0)
        self._session = requests.Session()
        self._session.headers.update(self.HEADERS)

        self._cache_dir = cache_dir
        if settings.environment == "dev" and cache_dir is None:
            self._cache_dir = PROJECT_ROOT / ".cache" / "fbref"
            self._cache_dir.mkdir(parents=True, exist_ok=True)

    @retry(
        retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
        wait=wait_exponential(multiplier=2, min=5, max=120),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get(self, url: str) -> str:
        """Fetch a URL with rate limiting and optional caching.

        Returns raw HTML content.
        """
        # Check cache first in dev
        if self._cache_dir:
            cached = self._read_cache(url)
            if cached:
                log.debug("cache_hit", url=url)
                return cached

        self._limiter.acquire()
        log.info("fetching_fbref", url=url)

        response = self._session.get(url, timeout=30)

        if response.status_code == 429:
            log.warning("rate_limited", url=url)
            raise requests.HTTPError(response=response)

        response.raise_for_status()
        html = response.text

        # Write to cache in dev
        if self._cache_dir:
            self._write_cache(url, html)

        return html

    def _cache_key(self, url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()

    def _read_cache(self, url: str) -> str | None:
        if not self._cache_dir:
            return None
        path = self._cache_dir / f"{self._cache_key(url)}.html"
        if path.exists():
            return path.read_text()
        return None

    def _write_cache(self, url: str, content: str) -> None:
        if not self._cache_dir:
            return
        path = self._cache_dir / f"{self._cache_key(url)}.html"
        path.write_text(content)
