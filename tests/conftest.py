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
def tmp_duckdb(tmp_path):
    """Create a temporary DuckDB database."""
    from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
    from sports_pipeline.loaders.views import init_schema

    db_path = tmp_path / "test.duckdb"
    loader = DuckDBLoader(db_path=db_path)
    init_schema(loader)
    return loader
