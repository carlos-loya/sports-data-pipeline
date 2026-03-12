"""NBA game data transformer (Bronze -> Silver)."""

from __future__ import annotations

import pandas as pd

from sports_pipeline.transformers.base import BaseTransformer
from sports_pipeline.transformers.common.deduplicator import deduplicate
from sports_pipeline.transformers.common.name_normalizer import NameNormalizer


class NbaGameTransformer(BaseTransformer):
    """Transform bronze NBA game data to silver layer."""

    def __init__(self) -> None:
        super().__init__()
        self.normalizer = NameNormalizer()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        self.log.info("transforming_nba_games", rows=len(df))

        df["home_team"] = df["home_team_name"].apply(
            lambda x: self.normalizer.normalize_team(str(x), "basketball")
        )
        df["away_team"] = df["away_team_name"].apply(
            lambda x: self.normalizer.normalize_team(str(x), "basketball")
        )

        silver = pd.DataFrame({
            "game_id": df["game_id"],
            "season": df["season"],
            "game_date": pd.to_datetime(df["game_date"]).dt.date,
            "home_team_id": df["home_team_id"].astype(int),
            "home_team": df["home_team"],
            "away_team_id": df["away_team_id"].astype(int),
            "away_team": df["away_team"],
            "home_score": pd.to_numeric(df["home_score"], errors="coerce").astype("Int64"),
            "away_score": pd.to_numeric(df["away_score"], errors="coerce").astype("Int64"),
        })

        # Derive fields
        silver["home_win"] = silver.apply(
            lambda r: r["home_score"] > r["away_score"]
            if pd.notna(r["home_score"]) and pd.notna(r["away_score"])
            else None,
            axis=1,
        )
        silver["total_points"] = silver.apply(
            lambda r: r["home_score"] + r["away_score"]
            if pd.notna(r["home_score"]) and pd.notna(r["away_score"])
            else None,
            axis=1,
        )

        silver = deduplicate(silver, subset=["game_id"])
        self.log.info("transformed_nba_games", rows=len(silver))
        return silver
