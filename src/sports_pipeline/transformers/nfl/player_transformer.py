"""NFL player game data transformer (Bronze -> Silver)."""

from __future__ import annotations

import pandas as pd

from sports_pipeline.transformers.base import BaseTransformer
from sports_pipeline.transformers.common.deduplicator import deduplicate
from sports_pipeline.transformers.common.name_normalizer import NameNormalizer


class NflPlayerTransformer(BaseTransformer):
    """Transform bronze NFL player game data to silver layer."""

    def __init__(self) -> None:
        super().__init__()
        self.normalizer = NameNormalizer()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        self.log.info("transforming_nfl_players", rows=len(df))

        df = df.copy()
        df["team_canonical"] = df["team"].apply(
            lambda x: self.normalizer.normalize_team(str(x), "football")
            if pd.notna(x) else x
        )

        # Normalize player names: use display name, strip whitespace
        player_name = df["player_display_name"].str.strip()

        passing_yards = pd.to_numeric(df.get("passing_yards", 0), errors="coerce").fillna(0.0)
        rushing_yards = pd.to_numeric(df.get("rushing_yards", 0), errors="coerce").fillna(0.0)
        receiving_yards = pd.to_numeric(df.get("receiving_yards", 0), errors="coerce").fillna(0.0)
        passing_tds = pd.to_numeric(df.get("passing_tds", 0), errors="coerce").fillna(0).astype(int)
        rushing_tds = pd.to_numeric(df.get("rushing_tds", 0), errors="coerce").fillna(0).astype(int)
        receiving_tds = pd.to_numeric(df.get("receiving_tds", 0), errors="coerce").fillna(0).astype(int)

        silver = pd.DataFrame({
            "player_id": df["player_id"],
            "season": df["season"].astype(int),
            "week": df["week"].astype(int),
            "player_name": player_name,
            "team": df["team_canonical"],
            "position": df.get("position"),
            # Passing
            "completions": pd.to_numeric(df.get("completions", 0), errors="coerce").fillna(0).astype(int),
            "passing_attempts": pd.to_numeric(df.get("passing_attempts", 0), errors="coerce").fillna(0).astype(int),
            "passing_yards": passing_yards,
            "passing_tds": passing_tds,
            "interceptions": pd.to_numeric(df.get("interceptions", 0), errors="coerce").fillna(0).astype(int),
            "sacks": pd.to_numeric(df.get("sacks", 0), errors="coerce").fillna(0).astype(int),
            # Rushing
            "carries": pd.to_numeric(df.get("carries", 0), errors="coerce").fillna(0).astype(int),
            "rushing_yards": rushing_yards,
            "rushing_tds": rushing_tds,
            "rushing_fumbles": pd.to_numeric(df.get("rushing_fumbles", 0), errors="coerce").fillna(0).astype(int),
            # Receiving
            "receptions": pd.to_numeric(df.get("receptions", 0), errors="coerce").fillna(0).astype(int),
            "targets": pd.to_numeric(df.get("targets", 0), errors="coerce").fillna(0).astype(int),
            "receiving_yards": receiving_yards,
            "receiving_tds": receiving_tds,
            # Fantasy
            "fantasy_points": pd.to_numeric(df.get("fantasy_points", 0), errors="coerce").fillna(0.0),
            # Derived
            "total_yards": passing_yards + rushing_yards + receiving_yards,
            "total_tds": passing_tds + rushing_tds + receiving_tds,
        })

        silver = deduplicate(silver, subset=["player_id", "season", "week"])
        self.log.info("transformed_nfl_players", rows=len(silver))
        return silver
