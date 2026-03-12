"""Silver-layer Pydantic models for cleaned soccer data."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class SilverSoccerMatch(BaseModel):
    """Cleaned and normalized soccer match."""
    match_id: str
    season: str
    league: str
    match_date: date
    home_team: str  # Canonical name
    away_team: str  # Canonical name
    home_goals: int | None = None
    away_goals: int | None = None
    home_xg: float | None = None
    away_xg: float | None = None
    result: str | None = None  # "H", "D", "A"
    venue: str | None = None


class SilverSoccerPlayerMatch(BaseModel):
    """Cleaned player match stats."""
    match_id: str
    season: str
    league: str
    match_date: date
    player_name: str
    team: str
    opponent: str
    is_home: bool
    minutes: int = 0
    goals: int = 0
    assists: int = 0
    xg: float = 0.0
    xa: float = 0.0
    shots: int = 0
    shots_on_target: int = 0
    key_passes: int = 0
    tackles: int = 0
    interceptions: int = 0
