"""Soccer player match data transformer (Bronze -> Silver)."""

from __future__ import annotations

import pandas as pd

from sports_pipeline.transformers.base import BaseTransformer
from sports_pipeline.transformers.common.name_normalizer import NameNormalizer


class SoccerPlayerTransformer(BaseTransformer):
    """Transform bronze soccer player data to silver layer."""

    def __init__(self) -> None:
        super().__init__()
        self.normalizer = NameNormalizer()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        self.log.info("transforming_soccer_players", rows=len(df))

        df["team"] = df["team"].apply(lambda x: self.normalizer.normalize_team(str(x), "soccer"))
        df["opponent"] = df["opponent"].apply(
            lambda x: self.normalizer.normalize_team(str(x), "soccer")
        )

        silver = pd.DataFrame({
            "match_id": df.get("match_id", ""),
            "season": df["season"],
            "league": df["league"],
            "match_date": pd.to_datetime(df["match_date"], errors="coerce").dt.date,
            "player_name": df["player_name"].str.strip(),
            "team": df["team"],
            "opponent": df["opponent"],
            "is_home": df["is_home"].astype(bool),
            "minutes": pd.to_numeric(df.get("minutes", 0), errors="coerce").fillna(0).astype(int),
            "goals": pd.to_numeric(df.get("goals", 0), errors="coerce").fillna(0).astype(int),
            "assists": pd.to_numeric(df.get("assists", 0), errors="coerce").fillna(0).astype(int),
            "xg": pd.to_numeric(df.get("xg", 0), errors="coerce").fillna(0.0),
            "xa": pd.to_numeric(df.get("xa", 0), errors="coerce").fillna(0.0),
            "shots": pd.to_numeric(df.get("shots", 0), errors="coerce").fillna(0).astype(int),
            "shots_on_target": pd.to_numeric(
                df.get("shots_on_target", 0), errors="coerce"
            ).fillna(0).astype(int),
            "key_passes": pd.to_numeric(
                df.get("key_passes", 0), errors="coerce"
            ).fillna(0).astype(int),
            "tackles": pd.to_numeric(
                df.get("tackles", 0), errors="coerce"
            ).fillna(0).astype(int),
            "interceptions": pd.to_numeric(
                df.get("interceptions", 0), errors="coerce"
            ).fillna(0).astype(int),
        })

        self.log.info("transformed_soccer_players", rows=len(silver))
        return silver
