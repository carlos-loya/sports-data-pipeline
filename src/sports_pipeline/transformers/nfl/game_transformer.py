"""NFL game data transformer (Bronze -> Silver)."""

from __future__ import annotations

import pandas as pd

from sports_pipeline.transformers.base import BaseTransformer
from sports_pipeline.transformers.common.deduplicator import deduplicate
from sports_pipeline.transformers.common.name_normalizer import NameNormalizer


class NflGameTransformer(BaseTransformer):
    """Transform bronze NFL game data to silver layer."""

    def __init__(self) -> None:
        super().__init__()
        self.normalizer = NameNormalizer()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        self.log.info("transforming_nfl_games", rows=len(df))

        df = df.copy()
        df["home_team_canonical"] = df["home_team"].apply(
            lambda x: self.normalizer.normalize_team(str(x), "football")
        )
        df["away_team_canonical"] = df["away_team"].apply(
            lambda x: self.normalizer.normalize_team(str(x), "football")
        )

        silver = pd.DataFrame(
            {
                "game_id": df["game_id"],
                "season": df["season"].astype(int),
                "week": df["week"].astype(int),
                "game_date": pd.to_datetime(df["gameday"], errors="coerce").dt.date,
                "home_team": df["home_team_canonical"],
                "away_team": df["away_team_canonical"],
                "home_score": pd.to_numeric(df["home_score"], errors="coerce").astype("Int64"),
                "away_score": pd.to_numeric(df["away_score"], errors="coerce").astype("Int64"),
                "overtime": df["overtime"].astype(bool),
                "game_type": df.get("game_type"),
                "stadium": df.get("stadium"),
            }
        )

        # Derive fields
        silver["home_win"] = silver.apply(
            lambda r: (
                r["home_score"] > r["away_score"]
                if pd.notna(r["home_score"]) and pd.notna(r["away_score"])
                else None
            ),
            axis=1,
        )
        silver["total_points"] = silver.apply(
            lambda r: (
                r["home_score"] + r["away_score"]
                if pd.notna(r["home_score"]) and pd.notna(r["away_score"])
                else None
            ),
            axis=1,
        )

        silver = deduplicate(silver, subset=["game_id"])
        self.log.info("transformed_nfl_games", rows=len(silver))
        return silver
