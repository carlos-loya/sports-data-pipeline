"""NFL player game stats extractor."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from sports_pipeline.extractors.base import BaseExtractor
from sports_pipeline.extractors.nfl.client import NflClient


class NflPlayerExtractor(BaseExtractor):
    """Extract NFL player game-level stats from nflreadpy."""

    def __init__(self, client: NflClient | None = None) -> None:
        super().__init__()
        self.client = client or NflClient()

    def extract(self, season: int, week: int | None = None) -> pd.DataFrame:
        """Extract player stats for a season, optionally filtered to a single week.

        Args:
            season: NFL season year (e.g. 2024)
            week: If provided, only return stats for this week (incremental).

        Returns:
            DataFrame with bronze-level player game data.
        """
        self.log.info("extracting_nfl_player_stats", season=season, week=week)
        raw = self.client.get_player_stats(season=season, summary_level="week")

        if raw.empty:
            self.log.warning("no_nfl_player_stats_found", season=season)
            return pd.DataFrame()

        if week is not None:
            raw = raw[raw["week"] == week]
            if raw.empty:
                self.log.warning("no_nfl_player_stats_for_week", season=season, week=week)
                return pd.DataFrame()

        result = self._transform(raw, season)
        self.log.info("extracted_nfl_player_stats", season=season, week=week, count=len(result))
        return result

    def _transform(self, df: pd.DataFrame, season: int) -> pd.DataFrame:
        """Map nflreadpy player stats columns to bronze schema."""
        now = datetime.now(UTC)

        return pd.DataFrame(
            {
                "extract_timestamp": now,
                "season": season,
                "week": df["week"],
                "player_id": df["player_id"],
                "player_name": df["player_name"],
                "player_display_name": df["player_display_name"],
                "team": df.get("team"),
                "position": df.get("position"),
                # Passing
                "completions": df.get("completions"),
                "passing_attempts": df.get("attempts"),
                "passing_yards": df.get("passing_yards"),
                "passing_tds": df.get("passing_tds"),
                "interceptions": df.get("interceptions"),
                "sacks": df.get("sacks"),
                "passing_air_yards": df.get("passing_air_yards"),
                "passing_yards_after_catch": df.get("passing_yards_after_catch"),
                "passing_2pt_conversions": df.get("passing_2pt_conversions"),
                # Rushing
                "carries": df.get("carries"),
                "rushing_yards": df.get("rushing_yards"),
                "rushing_tds": df.get("rushing_tds"),
                "rushing_fumbles": df.get("rushing_fumbles"),
                "rushing_2pt_conversions": df.get("rushing_2pt_conversions"),
                # Receiving
                "receptions": df.get("receptions"),
                "targets": df.get("targets"),
                "receiving_yards": df.get("receiving_yards"),
                "receiving_tds": df.get("receiving_tds"),
                "receiving_air_yards": df.get("receiving_air_yards"),
                "receiving_yards_after_catch": df.get("receiving_yards_after_catch"),
                "receiving_fumbles": df.get("receiving_fumbles"),
                "receiving_2pt_conversions": df.get("receiving_2pt_conversions"),
                # Fantasy
                "fantasy_points": df.get("fantasy_points"),
            }
        )
