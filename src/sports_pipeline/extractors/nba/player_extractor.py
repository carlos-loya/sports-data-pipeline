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

    def extract(self, player_id: int, season: str, player_name: str = "") -> pd.DataFrame:
        """Extract game log for a single player.

        Args:
            player_id: NBA player ID
            season: NBA season string, e.g. "2024-25"
            player_name: Player's display name (the API response does not include it)

        Returns:
            DataFrame with bronze-level player game data.
        """
        self.log.info("extracting_player_games", player_id=player_id, season=season)
        raw = self.client.get_player_game_log(player_id=player_id, season=season)

        if not raw:
            self.log.warning("no_player_games_found", player_id=player_id, season=season)
            return pd.DataFrame()

        df = pd.DataFrame(raw)
        return self._transform_raw(df, season, player_name)

    def _transform_raw(self, df: pd.DataFrame, season: str, player_name: str = "") -> pd.DataFrame:
        """Map raw nba_api columns to bronze schema.

        Note: The PlayerGameLog endpoint does not return PLAYER_NAME, TEAM_ID,
        or TEAM_NAME. The player name must be passed by the caller, and the
        team abbreviation is derived from the MATCHUP column.
        """
        now = datetime.now(UTC)

        df["is_home"] = df["MATCHUP"].str.contains("vs.", regex=False)
        # MATCHUP is like "LAL vs. HOU" or "LAL @ BOS" — team abbrev is first token
        df["_team_abbr"] = df["MATCHUP"].str.split(r"\s+", n=1).str[0]

        return pd.DataFrame(
            {
                "extract_timestamp": now,
                "season": season,
                "game_id": df["Game_ID"],
                "game_date": pd.to_datetime(df["GAME_DATE"]),
                "player_id": df["Player_ID"],
                "player_name": player_name,
                "team_name": df["_team_abbr"],
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
