"""FBref URL construction utilities."""

from __future__ import annotations

FBREF_BASE = "https://fbref.com/en"


def league_season_url(league_id: str, season: str) -> str:
    """Construct URL for a league season page."""
    return f"{FBREF_BASE}/comps/{league_id}/{season}/{season}-Stats"


def match_report_url(match_path: str) -> str:
    """Construct full URL for a match report."""
    if match_path.startswith("http"):
        return match_path
    return f"{FBREF_BASE}{match_path}"


def schedule_url(league_id: str, season: str) -> str:
    """Construct URL for league schedule/results page."""
    return f"{FBREF_BASE}/comps/{league_id}/{season}/schedule/{season}-Scores-and-Fixtures"


def player_match_stats_url(match_path: str) -> str:
    """Construct URL for player match stats."""
    return match_report_url(match_path)


def team_stats_url(league_id: str, season: str) -> str:
    """Construct URL for league team stats."""
    return f"{FBREF_BASE}/comps/{league_id}/{season}/stats/{season}-Stats"


def shooting_stats_url(league_id: str, season: str) -> str:
    """Construct URL for league shooting stats (xG)."""
    return f"{FBREF_BASE}/comps/{league_id}/{season}/shooting/{season}-Shooting"
