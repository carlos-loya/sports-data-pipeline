"""NFL game data extractor."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from sports_pipeline.extractors.base import BaseExtractor
from sports_pipeline.extractors.nfl.client import NflClient


class NflGameExtractor(BaseExtractor):
    """Extract NFL game data from nflreadpy schedules."""

    def __init__(self, client: NflClient | None = None) -> None:
        super().__init__()
        self.client = client or NflClient()

    def extract(self, season: int, week: int | None = None) -> pd.DataFrame:
        """Extract games for a season, optionally filtered to a single week.

        Args:
            season: NFL season year (e.g. 2024)
            week: If provided, only return games for this week (incremental).

        Returns:
            DataFrame with bronze-level game data.
        """
        self.log.info("extracting_nfl_games", season=season, week=week)
        raw = self.client.get_schedules(season=season)

        if raw.empty:
            self.log.warning("no_nfl_games_found", season=season)
            return pd.DataFrame()

        if week is not None:
            raw = raw[raw["week"] == week]
            if raw.empty:
                self.log.warning("no_nfl_games_for_week", season=season, week=week)
                return pd.DataFrame()

        result = self._transform(raw, season)
        self.log.info("extracted_nfl_games", season=season, week=week, count=len(result))
        return result

    def _transform(self, df: pd.DataFrame, season: int) -> pd.DataFrame:
        """Map nflreadpy schedule columns to bronze schema."""
        now = datetime.utcnow()

        return pd.DataFrame({
            "extract_timestamp": now,
            "season": season,
            "week": df["week"],
            "game_id": df["game_id"],
            "gameday": pd.to_datetime(df["gameday"], errors="coerce"),
            "gametime": df.get("gametime"),
            "home_team": df["home_team"],
            "away_team": df["away_team"],
            "home_score": df.get("home_score"),
            "away_score": df.get("away_score"),
            "overtime": df.get("overtime", pd.Series(dtype="object")).notna()
            & (df.get("overtime", pd.Series(dtype="object")) != ""),
            "game_type": df.get("game_type"),
            "location": df.get("location"),
            "stadium": df.get("stadium"),
            "roof": df.get("roof"),
            "surface": df.get("surface"),
            "spread_line": df.get("spread_line"),
            "total_line": df.get("total_line"),
            "result": df.get("result"),
        })
