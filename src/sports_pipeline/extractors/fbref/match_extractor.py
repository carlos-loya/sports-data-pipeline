"""FBref match data extractor."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from sports_pipeline.extractors.base import BaseExtractor
from sports_pipeline.extractors.fbref.client import FbrefClient
from sports_pipeline.extractors.fbref.parser import FbrefParser
from sports_pipeline.extractors.fbref.urls import schedule_url


class FbrefMatchExtractor(BaseExtractor):
    """Extract match results from FBref schedule pages."""

    def __init__(self, client: FbrefClient | None = None) -> None:
        super().__init__()
        self.client = client or FbrefClient()
        self.parser = FbrefParser()

    def extract(self, league_id: str, season: str, league_name: str = "") -> pd.DataFrame:
        """Extract match results for a league season.

        Args:
            league_id: FBref league ID
            season: Season string, e.g. "2024-2025"
            league_name: Human-readable league name

        Returns:
            DataFrame of bronze-level match data.
        """
        self.log.info("extracting_fbref_matches", league_id=league_id, season=season)
        url = schedule_url(league_id, season)
        html = self.client.get(url)

        df = self.parser.parse_table(html, table_id="sched_all")
        if df is None or df.empty:
            self.log.warning("no_matches_found", league_id=league_id, season=season)
            return pd.DataFrame()

        return self._transform_raw(df, season, league_name)

    def _transform_raw(self, df: pd.DataFrame, season: str, league_name: str) -> pd.DataFrame:
        """Map FBref schedule table to bronze schema."""
        now = datetime.utcnow()

        # FBref schedule columns: Date, Time, Home, Score, Away, ...
        # Filter to rows that have a score (completed matches)
        if "Score" not in df.columns:
            return pd.DataFrame()

        completed = df[df["Score"].notna() & df["Score"].str.contains("\u2013", na=False)].copy()

        if completed.empty:
            return pd.DataFrame()

        # Parse score "2\u20131" into home_goals, away_goals
        scores = completed["Score"].str.split("\u2013", expand=True)
        completed["home_goals"] = pd.to_numeric(scores[0].str.strip(), errors="coerce")
        completed["away_goals"] = pd.to_numeric(scores[1].str.strip(), errors="coerce")

        result = pd.DataFrame({
            "extract_timestamp": now,
            "season": season,
            "league": league_name,
            "match_date": pd.to_datetime(completed.get("Date", ""), errors="coerce"),
            "home_team": completed.get("Home", ""),
            "away_team": completed.get("Away", ""),
            "home_goals": completed["home_goals"],
            "away_goals": completed["away_goals"],
            "home_xg": (
                pd.to_numeric(completed.get("xG", ""), errors="coerce")
                if "xG" in completed.columns else None
            ),
            "away_xg": (
                pd.to_numeric(completed.get("xG.1", ""), errors="coerce")
                if "xG.1" in completed.columns else None
            ),
            "venue": completed.get("Venue", None),
            "referee": completed.get("Referee", None),
            "attendance": (
                pd.to_numeric(
                    completed.get("Attendance", "").str.replace(",", ""),
                    errors="coerce",
                ) if "Attendance" in completed.columns else None
            ),
            "match_url": None,
        })

        result = result.dropna(subset=["match_date", "home_team", "away_team"])
        self.log.info("extracted_fbref_matches", count=len(result))
        return result
