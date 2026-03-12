"""FBref player match stats extractor."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from sports_pipeline.extractors.base import BaseExtractor
from sports_pipeline.extractors.fbref.client import FbrefClient
from sports_pipeline.extractors.fbref.parser import FbrefParser
from sports_pipeline.extractors.fbref.urls import match_report_url


class FbrefPlayerExtractor(BaseExtractor):
    """Extract player-level match stats from FBref match reports."""

    STAT_TABLES = [
        "stats_{team_id}_summary",
        "stats_{team_id}_passing",
        "stats_{team_id}_defense",
    ]

    def __init__(self, client: FbrefClient | None = None) -> None:
        super().__init__()
        self.client = client or FbrefClient()
        self.parser = FbrefParser()

    def extract(self, match_url: str, season: str, league: str) -> pd.DataFrame:
        """Extract player stats from a single match report page.

        Args:
            match_url: FBref match report URL or path
            season: Season string
            league: League name

        Returns:
            DataFrame of bronze-level player match data.
        """
        self.log.info("extracting_player_stats", url=match_url)
        url = match_report_url(match_url)
        html = self.client.get(url)

        tables = self.parser.parse_all_tables(html)

        # Look for summary stats tables (contain player-level data)
        dfs = []
        for table_id, df in tables.items():
            if "summary" in table_id and "Player" in df.columns:
                dfs.append(df)

        if not dfs:
            self.log.warning("no_player_tables", url=match_url)
            return pd.DataFrame()

        combined = pd.concat(dfs, ignore_index=True)
        combined = combined[combined["Player"].notna()]

        now = datetime.utcnow()
        result = pd.DataFrame({
            "extract_timestamp": now,
            "season": season,
            "league": league,
            "match_date": None,
            "player_name": combined.get("Player", ""),
            "team": "",
            "opponent": "",
            "is_home": False,
            "minutes": pd.to_numeric(combined.get("Min", 0), errors="coerce"),
            "goals": pd.to_numeric(combined.get("Gls", 0), errors="coerce"),
            "assists": pd.to_numeric(combined.get("Ast", 0), errors="coerce"),
            "shots": pd.to_numeric(combined.get("Sh", 0), errors="coerce"),
            "shots_on_target": pd.to_numeric(combined.get("SoT", 0), errors="coerce"),
            "xg": pd.to_numeric(combined.get("xG", 0), errors="coerce"),
            "xa": pd.to_numeric(combined.get("xAG", 0), errors="coerce"),
        })

        self.log.info("extracted_player_stats", count=len(result))
        return result
