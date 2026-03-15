"""NFL team season stats extractor (derived from schedule data)."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from sports_pipeline.extractors.base import BaseExtractor
from sports_pipeline.extractors.nfl.client import NflClient


class NflTeamExtractor(BaseExtractor):
    """Extract NFL team season stats derived from nflreadpy schedule data."""

    def __init__(self, client: NflClient | None = None) -> None:
        super().__init__()
        self.client = client or NflClient()

    def extract(self, season: int) -> pd.DataFrame:
        """Extract team stats for a season by aggregating game results.

        Args:
            season: NFL season year (e.g. 2024)

        Returns:
            DataFrame with bronze-level team stats.
        """
        self.log.info("extracting_nfl_team_stats", season=season)
        raw = self.client.get_schedules(season=season)

        if raw.empty:
            self.log.warning("no_nfl_games_found", season=season)
            return pd.DataFrame()

        # Only include completed games (both scores present)
        completed = raw.dropna(subset=["home_score", "away_score"])
        if completed.empty:
            self.log.warning("no_completed_nfl_games", season=season)
            return pd.DataFrame()

        result = self._aggregate(completed, season)
        self.log.info("extracted_nfl_team_stats", season=season, count=len(result))
        return result

    def _aggregate(self, df: pd.DataFrame, season: int) -> pd.DataFrame:
        """Aggregate game-level data into team season stats."""
        now = datetime.now(UTC)
        teams: dict[str, dict] = {}

        for _, row in df.iterrows():
            home = row["home_team"]
            away = row["away_team"]
            home_score = int(row["home_score"])
            away_score = int(row["away_score"])

            for team in [home, away]:
                if team not in teams:
                    teams[team] = {
                        "games_played": 0,
                        "wins": 0,
                        "losses": 0,
                        "ties": 0,
                        "points_for": 0,
                        "points_against": 0,
                        "home_wins": 0,
                        "home_losses": 0,
                        "away_wins": 0,
                        "away_losses": 0,
                    }

            # Home team stats
            teams[home]["games_played"] += 1
            teams[home]["points_for"] += home_score
            teams[home]["points_against"] += away_score
            if home_score > away_score:
                teams[home]["wins"] += 1
                teams[home]["home_wins"] += 1
            elif home_score < away_score:
                teams[home]["losses"] += 1
                teams[home]["home_losses"] += 1
            else:
                teams[home]["ties"] += 1

            # Away team stats
            teams[away]["games_played"] += 1
            teams[away]["points_for"] += away_score
            teams[away]["points_against"] += home_score
            if away_score > home_score:
                teams[away]["wins"] += 1
                teams[away]["away_wins"] += 1
            elif away_score < home_score:
                teams[away]["losses"] += 1
                teams[away]["away_losses"] += 1
            else:
                teams[away]["ties"] += 1

        rows = []
        for team, stats in teams.items():
            rows.append(
                {
                    "extract_timestamp": now,
                    "season": season,
                    "team": team,
                    "point_differential": stats["points_for"] - stats["points_against"],
                    **stats,
                }
            )

        return pd.DataFrame(rows)
