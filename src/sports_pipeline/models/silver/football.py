"""Silver-layer Pydantic models for cleaned football data."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class SilverNflGame(BaseModel):
    """Cleaned and normalized NFL game."""

    game_id: str
    season: int
    week: int
    game_date: date | None = None
    home_team: str  # Canonical name
    away_team: str  # Canonical name
    home_score: int | None = None
    away_score: int | None = None
    home_win: bool | None = None
    total_points: int | None = None
    overtime: bool = False
    game_type: str | None = None
    stadium: str | None = None


class SilverNflPlayerGame(BaseModel):
    """Cleaned and normalized NFL player game stats."""

    player_id: str
    season: int
    week: int
    player_name: str
    team: str  # Canonical name
    position: str | None = None
    # Passing
    completions: int = 0
    passing_attempts: int = 0
    passing_yards: float = 0.0
    passing_tds: int = 0
    interceptions: int = 0
    sacks: int = 0
    # Rushing
    carries: int = 0
    rushing_yards: float = 0.0
    rushing_tds: int = 0
    rushing_fumbles: int = 0
    # Receiving
    receptions: int = 0
    targets: int = 0
    receiving_yards: float = 0.0
    receiving_tds: int = 0
    # Fantasy
    fantasy_points: float = 0.0
    # Derived
    total_yards: float = 0.0
    total_tds: int = 0
