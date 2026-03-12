"""NBA API client wrapper with retry and rate limiting."""

from __future__ import annotations

from typing import Any

from nba_api.stats.endpoints import (
    LeagueGameLog,
    PlayerGameLog,
    TeamEstimatedMetrics,
)
from nba_api.stats.static import teams as nba_teams
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from sports_pipeline.config import get_settings
from sports_pipeline.utils.logging import get_logger
from sports_pipeline.utils.rate_limiter import TokenBucketRateLimiter

log = get_logger(__name__)


class NbaApiClient:
    """Wrapper around nba_api with rate limiting and retry."""

    def __init__(self) -> None:
        settings = get_settings()
        rpm = settings.rate_limits.nba_api.requests_per_minute
        self._limiter = TokenBucketRateLimiter(rate=rpm, per=60.0)

    @retry(
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get_league_game_log(
        self, season: str, season_type: str = "Regular Season"
    ) -> list[dict[str, Any]]:
        """Fetch all games for a season."""
        self._limiter.acquire()
        log.info("fetching_league_game_log", season=season, season_type=season_type)
        result = LeagueGameLog(
            season=season,
            season_type_all_star=season_type,
            timeout=30,
        )
        return result.get_normalized_dict()["LeagueGameLog"]

    @retry(
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get_player_game_log(self, player_id: int, season: str) -> list[dict[str, Any]]:
        """Fetch game log for a specific player."""
        self._limiter.acquire()
        log.info("fetching_player_game_log", player_id=player_id, season=season)
        result = PlayerGameLog(
            player_id=player_id,
            season=season,
            timeout=30,
        )
        return result.get_normalized_dict()["PlayerGameLog"]

    @retry(
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get_team_estimated_metrics(self, season: str) -> list[dict[str, Any]]:
        """Fetch team advanced/estimated metrics for a season."""
        self._limiter.acquire()
        log.info("fetching_team_metrics", season=season)
        result = TeamEstimatedMetrics(
            season=season,
            timeout=30,
        )
        return result.get_normalized_dict()["TeamEstimatedMetrics"]

    @staticmethod
    def get_all_teams() -> list[dict[str, Any]]:
        """Get static list of all NBA teams."""
        return nba_teams.get_teams()
