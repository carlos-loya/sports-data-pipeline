"""Data quality checks using Pandera."""

from __future__ import annotations

import pandera as pa
from pandera import Check, Column

bronze_nba_game_schema = pa.DataFrameSchema(
    {
        "game_id": Column(str, nullable=False),
        "game_date": Column("datetime64[ns]", nullable=False),
        "home_team_id": Column(int, nullable=False),
        "away_team_id": Column(int, nullable=False),
        "home_score": Column(int, Check.ge(0), nullable=True),
        "away_score": Column(int, Check.ge(0), nullable=True),
    },
    strict=False,
)
