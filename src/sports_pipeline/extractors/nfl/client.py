"""NFL data client wrapper using nflreadpy with retry and rate limiting."""

from __future__ import annotations

import nflreadpy
import pandas as pd
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from sports_pipeline.config import get_settings
from sports_pipeline.utils.logging import get_logger
from sports_pipeline.utils.rate_limiter import TokenBucketRateLimiter

log = get_logger(__name__)


class NflClient:
    """Wrapper around nflreadpy with rate limiting and retry."""

    def __init__(self) -> None:
        settings = get_settings()
        rpm = settings.rate_limits.nflreadpy.requests_per_minute
        self._limiter = TokenBucketRateLimiter(rate=rpm, per=60.0)

    @retry(
        retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get_schedules(self, season: int) -> pd.DataFrame:
        """Fetch all games/schedules for a season."""
        self._limiter.acquire()
        log.info("fetching_nfl_schedules", season=season)
        df = nflreadpy.load_schedules(seasons=[season])
        # nflreadpy returns Polars; convert to pandas
        if hasattr(df, "to_pandas"):
            df = df.to_pandas()
        return df

    @retry(
        retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get_player_stats(self, season: int, summary_level: str = "week") -> pd.DataFrame:
        """Fetch player stats for a season.

        Args:
            season: NFL season year (e.g. 2024)
            summary_level: Aggregation level — "week" for per-game stats.

        Returns:
            DataFrame with player statistics.
        """
        self._limiter.acquire()
        log.info("fetching_nfl_player_stats", season=season, summary_level=summary_level)
        df = nflreadpy.load_player_stats(seasons=[season], summary_level=summary_level)
        if hasattr(df, "to_pandas"):
            df = df.to_pandas()
        return df
