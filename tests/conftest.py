"""Shared test fixtures."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_nba_game_log():
    """Sample nba_api LeagueGameLog response."""
    return [
        {
            "GAME_ID": "0022400001",
            "GAME_DATE": "2024-10-22",
            "TEAM_ID": 1610612747,
            "TEAM_NAME": "Los Angeles Lakers",
            "MATCHUP": "LAL vs. BOS",
            "PTS": 110,
            "WL": "W",
            "MIN": 240,
        },
        {
            "GAME_ID": "0022400001",
            "GAME_DATE": "2024-10-22",
            "TEAM_ID": 1610612738,
            "TEAM_NAME": "Boston Celtics",
            "MATCHUP": "BOS @ LAL",
            "PTS": 105,
            "WL": "L",
            "MIN": 240,
        },
    ]


@pytest.fixture
def sample_nba_player_log():
    """Sample nba_api PlayerGameLog response."""
    return [
        {
            "Game_ID": "0022400001",
            "GAME_DATE": "2024-10-22",
            "Player_ID": 2544,
            "PLAYER_NAME": "LeBron James",
            "TEAM_ID": 1610612747,
            "TEAM_NAME": "Los Angeles Lakers",
            "MATCHUP": "LAL vs. BOS",
            "MIN": 36.0,
            "PTS": 28,
            "REB": 8,
            "AST": 10,
            "STL": 2,
            "BLK": 1,
            "TOV": 3,
            "FGM": 10,
            "FGA": 20,
            "FG3M": 3,
            "FG3A": 8,
            "FTM": 5,
            "FTA": 6,
            "PLUS_MINUS": 5.0,
        },
    ]


@pytest.fixture
def sample_nba_team_metrics():
    """Sample nba_api TeamEstimatedMetrics response."""
    return [
        {
            "TEAM_ID": 1610612747,
            "TEAM_NAME": "Los Angeles Lakers",
            "GP": 20,
            "W": 12,
            "L": 8,
            "E_OFF_RATING": 115.2,
            "E_DEF_RATING": 110.5,
            "E_NET_RATING": 4.7,
            "E_PACE": 101.3,
        },
        {
            "TEAM_ID": 1610612738,
            "TEAM_NAME": "Boston Celtics",
            "GP": 20,
            "W": 15,
            "L": 5,
            "E_OFF_RATING": 118.0,
            "E_DEF_RATING": 108.2,
            "E_NET_RATING": 9.8,
            "E_PACE": 99.5,
        },
    ]


@pytest.fixture
def sample_bronze_nba_games():
    """Sample bronze NBA games DataFrame."""
    return pd.DataFrame([
        {
            "extract_timestamp": datetime.utcnow(),
            "season": "2024-25",
            "game_id": "0022400001",
            "game_date": datetime(2024, 10, 22),
            "home_team_id": 1610612747,
            "home_team_name": "Los Angeles Lakers",
            "away_team_id": 1610612738,
            "away_team_name": "Boston Celtics",
            "home_score": 110,
            "away_score": 105,
            "status": "Final",
        },
    ])


@pytest.fixture
def sample_bronze_soccer_matches():
    """Sample bronze soccer matches DataFrame."""
    return pd.DataFrame([
        {
            "extract_timestamp": datetime.utcnow(),
            "season": "2024-2025",
            "league": "Premier League",
            "match_date": datetime(2024, 9, 14),
            "home_team": "Arsenal",
            "away_team": "Wolves",
            "home_goals": 2,
            "away_goals": 0,
            "home_xg": 2.1,
            "away_xg": 0.5,
            "venue": "Emirates Stadium",
            "referee": None,
            "attendance": None,
            "match_url": None,
        },
        {
            "extract_timestamp": datetime.utcnow(),
            "season": "2024-2025",
            "league": "Premier League",
            "match_date": datetime(2024, 9, 14),
            "home_team": "Manchester City",
            "away_team": "Brighton",
            "home_goals": 1,
            "away_goals": 1,
            "home_xg": 1.8,
            "away_xg": 1.2,
            "venue": "Etihad Stadium",
            "referee": None,
            "attendance": None,
            "match_url": None,
        },
    ])


@pytest.fixture
def sample_nfl_schedule():
    """Sample nflreadpy schedule response as a pandas DataFrame."""
    return pd.DataFrame([
        {
            "game_id": "2024_01_KC_BAL",
            "season": 2024,
            "week": 1,
            "gameday": "2024-09-05",
            "gametime": "20:20",
            "home_team": "BAL",
            "away_team": "KC",
            "home_score": 20,
            "away_score": 27,
            "overtime": None,
            "game_type": "REG",
            "location": "Home",
            "stadium": "M&T Bank Stadium",
            "roof": "outdoors",
            "surface": "grass",
            "spread_line": -3.0,
            "total_line": 46.5,
            "result": -7,
        },
        {
            "game_id": "2024_01_GB_PHI",
            "season": 2024,
            "week": 1,
            "gameday": "2024-09-06",
            "gametime": "20:15",
            "home_team": "PHI",
            "away_team": "GB",
            "home_score": 34,
            "away_score": 29,
            "overtime": None,
            "game_type": "REG",
            "location": "Home",
            "stadium": "Lincoln Financial Field",
            "roof": "outdoors",
            "surface": "grass",
            "spread_line": -2.5,
            "total_line": 48.5,
            "result": 5,
        },
        {
            "game_id": "2024_02_KC_CIN",
            "season": 2024,
            "week": 2,
            "gameday": "2024-09-15",
            "gametime": "16:25",
            "home_team": "CIN",
            "away_team": "KC",
            "home_score": 25,
            "away_score": 26,
            "overtime": "OT",
            "game_type": "REG",
            "location": "Home",
            "stadium": "Paycor Stadium",
            "roof": "outdoors",
            "surface": "grass",
            "spread_line": -1.0,
            "total_line": 47.0,
            "result": -1,
        },
    ])


@pytest.fixture
def sample_nfl_player_stats():
    """Sample nflreadpy player stats response as a pandas DataFrame."""
    return pd.DataFrame([
        {
            "player_id": "00-0036442",
            "player_name": "P.Mahomes",
            "player_display_name": "Patrick Mahomes",
            "recent_team": "KC",
            "position": "QB",
            "week": 1,
            "completions": 20,
            "attempts": 28,
            "passing_yards": 291.0,
            "passing_tds": 1,
            "interceptions": 0,
            "sacks": 2,
            "passing_air_yards": 180.5,
            "passing_yards_after_catch": 110.5,
            "passing_2pt_conversions": 0,
            "carries": 3,
            "rushing_yards": 24.0,
            "rushing_tds": 0,
            "rushing_fumbles": 0,
            "rushing_2pt_conversions": 0,
            "receptions": 0,
            "targets": 0,
            "receiving_yards": 0.0,
            "receiving_tds": 0,
            "receiving_air_yards": 0.0,
            "receiving_yards_after_catch": 0.0,
            "receiving_fumbles": 0,
            "receiving_2pt_conversions": 0,
            "fantasy_points": 19.64,
        },
        {
            "player_id": "00-0039337",
            "player_name": "J.Hurts",
            "player_display_name": "Jalen Hurts",
            "recent_team": "PHI",
            "position": "QB",
            "week": 1,
            "completions": 18,
            "attempts": 33,
            "passing_yards": 278.0,
            "passing_tds": 2,
            "interceptions": 0,
            "sacks": 1,
            "passing_air_yards": 200.0,
            "passing_yards_after_catch": 78.0,
            "passing_2pt_conversions": 0,
            "carries": 13,
            "rushing_yards": 84.0,
            "rushing_tds": 2,
            "rushing_fumbles": 0,
            "rushing_2pt_conversions": 0,
            "receptions": 0,
            "targets": 0,
            "receiving_yards": 0.0,
            "receiving_tds": 0,
            "receiving_air_yards": 0.0,
            "receiving_yards_after_catch": 0.0,
            "receiving_fumbles": 0,
            "receiving_2pt_conversions": 0,
            "fantasy_points": 33.52,
        },
        {
            "player_id": "00-0036442",
            "player_name": "P.Mahomes",
            "player_display_name": "Patrick Mahomes",
            "recent_team": "KC",
            "position": "QB",
            "week": 2,
            "completions": 18,
            "attempts": 25,
            "passing_yards": 151.0,
            "passing_tds": 2,
            "interceptions": 2,
            "sacks": 0,
            "passing_air_yards": 120.0,
            "passing_yards_after_catch": 31.0,
            "passing_2pt_conversions": 0,
            "carries": 5,
            "rushing_yards": 29.0,
            "rushing_tds": 0,
            "rushing_fumbles": 0,
            "rushing_2pt_conversions": 0,
            "receptions": 0,
            "targets": 0,
            "receiving_yards": 0.0,
            "receiving_tds": 0,
            "receiving_air_yards": 0.0,
            "receiving_yards_after_catch": 0.0,
            "receiving_fumbles": 0,
            "receiving_2pt_conversions": 0,
            "fantasy_points": 14.94,
        },
    ])


@pytest.fixture
def tmp_duckdb(tmp_path):
    """Create a temporary DuckDB database."""
    from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
    from sports_pipeline.loaders.views import init_schema

    db_path = tmp_path / "test.duckdb"
    loader = DuckDBLoader(db_path=db_path)
    init_schema(loader)
    return loader
