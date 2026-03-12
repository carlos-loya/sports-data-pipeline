"""Bronze-layer Pydantic models for soccer data (FBref)."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


class BronzeSoccerMatch(BaseModel):
    """Raw match data from FBref."""
    extract_timestamp: datetime
    season: str
    league: str
    match_date: date
    home_team: str
    away_team: str
    home_goals: int | None = None
    away_goals: int | None = None
    home_xg: float | None = None
    away_xg: float | None = None
    match_url: str | None = None
    referee: str | None = None
    venue: str | None = None
    attendance: int | None = None


class BronzeSoccerPlayerMatch(BaseModel):
    """Raw player match stats from FBref."""
    extract_timestamp: datetime
    season: str
    league: str
    match_date: date
    player_name: str
    team: str
    opponent: str
    is_home: bool
    minutes: int | None = None
    goals: int | None = None
    assists: int | None = None
    shots: int | None = None
    shots_on_target: int | None = None
    xg: float | None = None
    xa: float | None = None
    passes_completed: int | None = None
    passes_attempted: int | None = None
    progressive_passes: int | None = None
    carries: int | None = None
    progressive_carries: int | None = None
    tackles: int | None = None
    interceptions: int | None = None
    blocks: int | None = None
    yellow_cards: int | None = None
    red_cards: int | None = None


class BronzeSoccerTeamSeason(BaseModel):
    """Raw team season stats from FBref."""
    extract_timestamp: datetime
    season: str
    league: str
    team: str
    matches_played: int
    wins: int
    draws: int
    losses: int
    goals_for: int
    goals_against: int
    goal_difference: int
    points: int
    xg_for: float | None = None
    xg_against: float | None = None
