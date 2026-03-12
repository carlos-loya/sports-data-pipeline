"""Gold-layer Pydantic models for soccer analytics."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class GoldSoccerTeamForm(BaseModel):
    """Rolling team form metrics."""
    team: str
    league: str
    as_of_date: date
    last_n_matches: int = 5
    wins: int = 0
    draws: int = 0
    losses: int = 0
    goals_scored: int = 0
    goals_conceded: int = 0
    xg_for: float = 0.0
    xg_against: float = 0.0
    points: int = 0
    form_string: str = ""  # e.g. "WWDLW"


class GoldSoccerH2H(BaseModel):
    """Head-to-head record between two teams."""
    team_a: str
    team_b: str
    league: str
    total_matches: int = 0
    team_a_wins: int = 0
    draws: int = 0
    team_b_wins: int = 0
    team_a_goals: int = 0
    team_b_goals: int = 0
