"""Bronze-layer Pydantic models for basketball data (nba_api)."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


class BronzeNbaGame(BaseModel):
    """Raw game data from nba_api."""
    extract_timestamp: datetime
    season: str
    game_id: str
    game_date: date
    home_team_id: int
    home_team_name: str
    away_team_id: int
    away_team_name: str
    home_score: int | None = None
    away_score: int | None = None
    status: str | None = None


class BronzeNbaPlayerGame(BaseModel):
    """Raw player game stats from nba_api."""
    extract_timestamp: datetime
    season: str
    game_id: str
    game_date: date
    player_id: int
    player_name: str
    team_id: int
    team_name: str
    is_home: bool
    minutes: float | None = None
    points: int | None = None
    rebounds: int | None = None
    assists: int | None = None
    steals: int | None = None
    blocks: int | None = None
    turnovers: int | None = None
    field_goals_made: int | None = None
    field_goals_attempted: int | None = None
    three_pointers_made: int | None = None
    three_pointers_attempted: int | None = None
    free_throws_made: int | None = None
    free_throws_attempted: int | None = None
    plus_minus: float | None = None


class BronzeNbaTeamStats(BaseModel):
    """Raw team advanced stats from nba_api."""
    extract_timestamp: datetime
    season: str
    team_id: int
    team_name: str
    games_played: int
    wins: int
    losses: int
    offensive_rating: float | None = None
    defensive_rating: float | None = None
    net_rating: float | None = None
    pace: float | None = None
    field_goal_pct: float | None = None
    three_point_pct: float | None = None
