"""NBA game data extractor."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from sports_pipeline.extractors.base import BaseExtractor
from sports_pipeline.extractors.nba.client import NbaApiClient


class NbaGameExtractor(BaseExtractor):
    """Extract NBA game data from nba_api."""

    def __init__(self, client: NbaApiClient | None = None) -> None:
        super().__init__()
        self.client = client or NbaApiClient()

    def extract(self, season: str) -> pd.DataFrame:
        """Extract all games for a given season.

        Args:
            season: NBA season string, e.g. "2024-25"

        Returns:
            DataFrame with bronze-level game data.
        """
        self.log.info("extracting_nba_games", season=season)
        raw_games = self.client.get_league_game_log(season=season)

        if not raw_games:
            self.log.warning("no_games_found", season=season)
            return pd.DataFrame()

        df = pd.DataFrame(raw_games)

        # nba_api returns one row per team per game; we need to pair home/away
        games = self._pair_games(df, season)
        self.log.info("extracted_nba_games", season=season, count=len(games))
        return games

    def _pair_games(self, df: pd.DataFrame, season: str) -> pd.DataFrame:
        """Pair team game logs into home/away game records."""
        now = datetime.utcnow()

        # The MATCHUP column contains "TEAM vs. OPP" for home, "TEAM @ OPP" for away
        df["is_home"] = df["MATCHUP"].str.contains("vs.", regex=False)

        home = df[df["is_home"]].copy()
        away = df[~df["is_home"]].copy()

        home = home.rename(columns={
            "GAME_ID": "game_id",
            "GAME_DATE": "game_date",
            "TEAM_ID": "home_team_id",
            "TEAM_NAME": "home_team_name",
            "PTS": "home_score",
        })
        away = away.rename(columns={
            "TEAM_ID": "away_team_id",
            "TEAM_NAME": "away_team_name",
            "PTS": "away_score",
        })

        merged = home.merge(
            away[["GAME_ID", "away_team_id", "away_team_name", "away_score"]],
            left_on="game_id",
            right_on="GAME_ID",
            how="inner",
        )

        result = pd.DataFrame({
            "extract_timestamp": now,
            "season": season,
            "game_id": merged["game_id"],
            "game_date": pd.to_datetime(merged["game_date"]),
            "home_team_id": merged["home_team_id"],
            "home_team_name": merged["home_team_name"],
            "away_team_id": merged["away_team_id"],
            "away_team_name": merged["away_team_name"],
            "home_score": merged["home_score"],
            "away_score": merged["away_score"],
            "status": "Final",
        })

        return result
