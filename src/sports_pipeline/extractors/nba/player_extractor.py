"""NBA player game data extractor."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from sports_pipeline.extractors.base import BaseExtractor
from sports_pipeline.extractors.nba.client import NbaApiClient


class NbaPlayerExtractor(BaseExtractor):
    """Extract NBA player game log data from nba_api."""

    def __init__(self, client: NbaApiClient | None = None) -> None:
        super().__init__()
        self.client = client or NbaApiClient()

    def extract(self, season: str) -> pd.DataFrame:
        """Extract all player game logs for a season.

        Args:
            season: NBA season string, e.g. "2024-25"

        Returns:
            DataFrame with bronze-level player game data.
        """
        self.log.info("extracting_nba_player_games", season=season)
        raw = self.client.get_league_player_game_log(season=season)

        if not raw:
            self.log.warning("no_player_games_found", season=season)
            return pd.DataFrame()

        df = pd.DataFrame(raw)
        result = self._transform_raw(df, season)
        self.log.info("extracted_nba_player_games", season=season, count=len(result))
        return result

    def _transform_raw(self, df: pd.DataFrame, season: str) -> pd.DataFrame:
        """Map raw nba_api LeagueGameLog (player mode) columns to bronze schema."""
        now = datetime.now(UTC)

        df["is_home"] = df["MATCHUP"].str.contains("vs.", regex=False)

        return pd.DataFrame(
            {
                "extract_timestamp": now,
                "season": season,
                "game_id": df["GAME_ID"],
                "game_date": pd.to_datetime(df["GAME_DATE"]),
                "player_id": df["PLAYER_ID"],
                "player_name": df["PLAYER_NAME"],
                "team_id": df["TEAM_ID"],
                "team_name": df["TEAM_NAME"],
                "is_home": df["is_home"],
                "minutes": df["MIN"],
                "points": df["PTS"],
                "rebounds": df["REB"],
                "assists": df["AST"],
                "steals": df["STL"],
                "blocks": df["BLK"],
                "turnovers": df["TOV"],
                "field_goals_made": df["FGM"],
                "field_goals_attempted": df["FGA"],
                "three_pointers_made": df["FG3M"],
                "three_pointers_attempted": df["FG3A"],
                "free_throws_made": df["FTM"],
                "free_throws_attempted": df["FTA"],
                "plus_minus": df["PLUS_MINUS"],
            }
        )
