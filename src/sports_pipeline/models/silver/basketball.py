"""Silver-layer Pydantic models for cleaned basketball data."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class SilverNbaGame(BaseModel):
    """Cleaned and normalized NBA game."""
    game_id: str
    season: str
    game_date: date
    home_team_id: int
    home_team: str  # Canonical name
    away_team_id: int
    away_team: str  # Canonical name
    home_score: int | None = None
    away_score: int | None = None
    home_win: bool | None = None
    total_points: int | None = None


class SilverNbaPlayerGame(BaseModel):
    """Cleaned player game stats."""
    game_id: str
    season: str
    game_date: date
    player_id: int
    player_name: str
    team_id: int
    team: str
    is_home: bool
    minutes: float = 0.0
    points: int = 0
    rebounds: int = 0
    assists: int = 0
    steals: int = 0
    blocks: int = 0
    turnovers: int = 0
    field_goal_pct: float | None = None
    three_point_pct: float | None = None
    free_throw_pct: float | None = None
    plus_minus: float = 0.0
