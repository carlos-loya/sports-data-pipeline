"""Bronze-layer Pydantic models for football data (nflreadpy)."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


class BronzeNflGame(BaseModel):
    """Raw game data from nflreadpy schedules."""

    extract_timestamp: datetime
    season: int
    week: int
    game_id: str
    gameday: date | None = None
    gametime: str | None = None
    home_team: str
    away_team: str
    home_score: int | None = None
    away_score: int | None = None
    overtime: bool = False
    game_type: str | None = None
    location: str | None = None
    stadium: str | None = None
    roof: str | None = None
    surface: str | None = None
    spread_line: float | None = None
    total_line: float | None = None
    result: int | None = None


class BronzeNflPlayerGame(BaseModel):
    """Raw player game stats from nflreadpy."""

    extract_timestamp: datetime
    season: int
    week: int
    player_id: str
    player_name: str
    player_display_name: str
    team: str | None = None
    position: str | None = None
    # Passing
    completions: int | None = None
    passing_attempts: int | None = None
    passing_yards: float | None = None
    passing_tds: int | None = None
    interceptions: int | None = None
    sacks: int | None = None
    passing_air_yards: float | None = None
    passing_yards_after_catch: float | None = None
    passing_2pt_conversions: int | None = None
    # Rushing
    carries: int | None = None
    rushing_yards: float | None = None
    rushing_tds: int | None = None
    rushing_fumbles: int | None = None
    rushing_2pt_conversions: int | None = None
    # Receiving
    receptions: int | None = None
    targets: int | None = None
    receiving_yards: float | None = None
    receiving_tds: int | None = None
    receiving_air_yards: float | None = None
    receiving_yards_after_catch: float | None = None
    receiving_fumbles: int | None = None
    receiving_2pt_conversions: int | None = None
    # Fantasy
    fantasy_points: float | None = None


class BronzeNflTeamStats(BaseModel):
    """Raw team season stats derived from nflreadpy schedule data."""

    extract_timestamp: datetime
    season: int
    team: str
    games_played: int
    wins: int
    losses: int
    ties: int = 0
    points_for: int
    points_against: int
    point_differential: int
    home_wins: int = 0
    home_losses: int = 0
    away_wins: int = 0
    away_losses: int = 0
