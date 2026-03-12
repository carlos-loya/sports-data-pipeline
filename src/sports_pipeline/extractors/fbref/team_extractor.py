"""FBref team season stats extractor."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from sports_pipeline.extractors.base import BaseExtractor
from sports_pipeline.extractors.fbref.client import FbrefClient
from sports_pipeline.extractors.fbref.parser import FbrefParser
from sports_pipeline.extractors.fbref.urls import league_season_url


class FbrefTeamExtractor(BaseExtractor):
    """Extract team-level season stats from FBref."""

    def __init__(self, client: FbrefClient | None = None) -> None:
        super().__init__()
        self.client = client or FbrefClient()
        self.parser = FbrefParser()

    def extract(self, league_id: str, season: str, league_name: str = "") -> pd.DataFrame:
        """Extract team stats for a league season.

        Args:
            league_id: FBref league ID
            season: Season string, e.g. "2024-2025"
            league_name: Human-readable league name

        Returns:
            DataFrame of bronze-level team season data.
        """
        self.log.info("extracting_fbref_teams", league_id=league_id, season=season)
        url = league_season_url(league_id, season)
        html = self.client.get(url)

        # Try standard stats table or league standings
        df = self.parser.parse_table(html, "stats_standard")
        if df is None:
            df = self.parser.parse_table(html, "results")

        if df is None or df.empty:
            self.log.warning("no_team_stats_found", league_id=league_id, season=season)
            return pd.DataFrame()

        return self._transform_raw(df, season, league_name)

    def _transform_raw(self, df: pd.DataFrame, season: str, league_name: str) -> pd.DataFrame:
        """Map FBref team stats to bronze schema."""
        now = datetime.utcnow()

        # The "Squad" column contains the team name
        if "Squad" not in df.columns:
            return pd.DataFrame()

        result = pd.DataFrame({
            "extract_timestamp": now,
            "season": season,
            "league": league_name,
            "team": df["Squad"],
            "matches_played": pd.to_numeric(df.get("MP", 0), errors="coerce"),
            "wins": pd.to_numeric(df.get("W", 0), errors="coerce"),
            "draws": pd.to_numeric(df.get("D", 0), errors="coerce"),
            "losses": pd.to_numeric(df.get("L", 0), errors="coerce"),
            "goals_for": pd.to_numeric(df.get("GF", 0), errors="coerce"),
            "goals_against": pd.to_numeric(df.get("GA", 0), errors="coerce"),
            "goal_difference": pd.to_numeric(df.get("GD", 0), errors="coerce"),
            "points": pd.to_numeric(df.get("Pts", 0), errors="coerce"),
            "xg_for": pd.to_numeric(df.get("xG", pd.NA), errors="coerce"),
            "xg_against": pd.to_numeric(df.get("xGA", pd.NA), errors="coerce"),
        })

        result = result.dropna(subset=["team"])
        self.log.info("extracted_fbref_teams", count=len(result))
        return result
