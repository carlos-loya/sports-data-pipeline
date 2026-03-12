"""NBA player game data transformer (Bronze -> Silver)."""

from __future__ import annotations

import pandas as pd

from sports_pipeline.transformers.base import BaseTransformer
from sports_pipeline.transformers.common.name_normalizer import NameNormalizer


class NbaPlayerTransformer(BaseTransformer):
    """Transform bronze NBA player game data to silver layer."""

    def __init__(self) -> None:
        super().__init__()
        self.normalizer = NameNormalizer()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        self.log.info("transforming_nba_players", rows=len(df))

        df["team"] = df["team_name"].apply(
            lambda x: self.normalizer.normalize_team(str(x), "basketball")
        )

        silver = pd.DataFrame({
            "game_id": df["game_id"],
            "season": df["season"],
            "game_date": pd.to_datetime(df["game_date"], errors="coerce").dt.date,
            "player_id": df["player_id"].astype(int),
            "player_name": df["player_name"].str.strip(),
            "team_id": df["team_id"].astype(int),
            "team": df["team"],
            "is_home": df["is_home"].astype(bool),
            "minutes": pd.to_numeric(df.get("minutes", 0), errors="coerce").fillna(0.0),
            "points": pd.to_numeric(df.get("points", 0), errors="coerce").fillna(0).astype(int),
            "rebounds": pd.to_numeric(df.get("rebounds", 0), errors="coerce").fillna(0).astype(int),
            "assists": pd.to_numeric(df.get("assists", 0), errors="coerce").fillna(0).astype(int),
            "steals": pd.to_numeric(df.get("steals", 0), errors="coerce").fillna(0).astype(int),
            "blocks": pd.to_numeric(df.get("blocks", 0), errors="coerce").fillna(0).astype(int),
            "turnovers": pd.to_numeric(
                df.get("turnovers", 0), errors="coerce"
            ).fillna(0).astype(int),
        })

        # Derive percentages
        fgm = pd.to_numeric(df.get("field_goals_made", 0), errors="coerce").fillna(0)
        fga = pd.to_numeric(df.get("field_goals_attempted", 0), errors="coerce").fillna(0)
        tpm = pd.to_numeric(df.get("three_pointers_made", 0), errors="coerce").fillna(0)
        tpa = pd.to_numeric(df.get("three_pointers_attempted", 0), errors="coerce").fillna(0)
        ftm = pd.to_numeric(df.get("free_throws_made", 0), errors="coerce").fillna(0)
        fta = pd.to_numeric(df.get("free_throws_attempted", 0), errors="coerce").fillna(0)

        silver["field_goal_pct"] = (fgm / fga).where(fga > 0)
        silver["three_point_pct"] = (tpm / tpa).where(tpa > 0)
        silver["free_throw_pct"] = (ftm / fta).where(fta > 0)
        silver["plus_minus"] = pd.to_numeric(df.get("plus_minus", 0), errors="coerce").fillna(0.0)

        self.log.info("transformed_nba_players", rows=len(silver))
        return silver
