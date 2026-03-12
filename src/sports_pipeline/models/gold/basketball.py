"""Gold-layer Pydantic models for basketball analytics."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class GoldNbaTeamForm(BaseModel):
    """Rolling team form metrics."""
    team: str
    team_id: int
    as_of_date: date
    last_n_games: int = 10
    wins: int = 0
    losses: int = 0
    avg_points_scored: float = 0.0
    avg_points_allowed: float = 0.0
    avg_point_diff: float = 0.0
    offensive_rating: float | None = None
    defensive_rating: float | None = None
    pace: float | None = None
