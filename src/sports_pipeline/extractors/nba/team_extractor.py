"""NBA team stats extractor."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from sports_pipeline.extractors.base import BaseExtractor
from sports_pipeline.extractors.nba.client import NbaApiClient


class NbaTeamExtractor(BaseExtractor):
    """Extract NBA team advanced stats from nba_api."""

    def __init__(self, client: NbaApiClient | None = None) -> None:
        super().__init__()
        self.client = client or NbaApiClient()

    def extract(self, season: str) -> pd.DataFrame:
        """Extract team advanced metrics for a season.

        Args:
            season: NBA season string, e.g. "2024-25"

        Returns:
            DataFrame with bronze-level team stats.
        """
        self.log.info("extracting_team_stats", season=season)
        raw = self.client.get_team_estimated_metrics(season=season)

        if not raw:
            self.log.warning("no_team_stats_found", season=season)
            return pd.DataFrame()

        df = pd.DataFrame(raw)
        return self._transform_raw(df, season)

    def _transform_raw(self, df: pd.DataFrame, season: str) -> pd.DataFrame:
        """Map raw nba_api columns to bronze schema."""
        now = datetime.utcnow()

        return pd.DataFrame({
            "extract_timestamp": now,
            "season": season,
            "team_id": df["TEAM_ID"],
            "team_name": df["TEAM_NAME"],
            "games_played": df["GP"],
            "wins": df["W"],
            "losses": df["L"],
            "offensive_rating": df.get("E_OFF_RATING"),
            "defensive_rating": df.get("E_DEF_RATING"),
            "net_rating": df.get("E_NET_RATING"),
            "pace": df.get("E_PACE"),
        })
