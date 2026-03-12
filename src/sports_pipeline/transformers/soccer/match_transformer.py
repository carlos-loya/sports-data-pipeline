"""Soccer match data transformer (Bronze -> Silver)."""

from __future__ import annotations

import hashlib

import pandas as pd

from sports_pipeline.transformers.base import BaseTransformer
from sports_pipeline.transformers.common.deduplicator import deduplicate
from sports_pipeline.transformers.common.name_normalizer import NameNormalizer


class SoccerMatchTransformer(BaseTransformer):
    """Transform bronze soccer match data to silver layer."""

    def __init__(self) -> None:
        super().__init__()
        self.normalizer = NameNormalizer()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform bronze match data into silver schema."""
        if df.empty:
            return pd.DataFrame()

        self.log.info("transforming_soccer_matches", rows=len(df))

        # Normalize team names
        df["home_team"] = df["home_team"].apply(
            lambda x: self.normalizer.normalize_team(str(x), "soccer")
        )
        df["away_team"] = df["away_team"].apply(
            lambda x: self.normalizer.normalize_team(str(x), "soccer")
        )

        # Generate match ID
        df["match_id"] = df.apply(self._generate_match_id, axis=1)

        # Derive result
        df["result"] = df.apply(self._derive_result, axis=1)

        # Select silver columns
        silver = pd.DataFrame({
            "match_id": df["match_id"],
            "season": df["season"],
            "league": df["league"],
            "match_date": pd.to_datetime(df["match_date"]).dt.date,
            "home_team": df["home_team"],
            "away_team": df["away_team"],
            "home_goals": df["home_goals"].astype("Int64"),
            "away_goals": df["away_goals"].astype("Int64"),
            "home_xg": pd.to_numeric(df.get("home_xg"), errors="coerce"),
            "away_xg": pd.to_numeric(df.get("away_xg"), errors="coerce"),
            "result": df["result"],
            "venue": df.get("venue"),
        })

        silver = deduplicate(silver, subset=["match_id"])
        self.log.info("transformed_soccer_matches", rows=len(silver))
        return silver

    @staticmethod
    def _generate_match_id(row) -> str:
        key = f"{row['season']}_{row.get('match_date', '')}_{row['home_team']}_{row['away_team']}"
        return hashlib.md5(key.encode()).hexdigest()[:12]

    @staticmethod
    def _derive_result(row) -> str | None:
        hg = row.get("home_goals")
        ag = row.get("away_goals")
        if pd.isna(hg) or pd.isna(ag):
            return None
        if hg > ag:
            return "H"
        elif hg < ag:
            return "A"
        return "D"
