"""DuckDB views for BI and analytics."""

from __future__ import annotations

from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)

SCHEMA_DDL = """
CREATE SCHEMA IF NOT EXISTS gold;

-- Core sports tables
CREATE TABLE IF NOT EXISTS gold.soccer_matches (
    match_id VARCHAR PRIMARY KEY,
    season VARCHAR,
    league VARCHAR,
    match_date DATE,
    home_team VARCHAR NOT NULL,
    away_team VARCHAR NOT NULL,
    home_goals INTEGER,
    away_goals INTEGER,
    home_xg DOUBLE,
    away_xg DOUBLE,
    result VARCHAR,
    venue VARCHAR
);

CREATE TABLE IF NOT EXISTS gold.soccer_player_matches (
    match_id VARCHAR,
    season VARCHAR,
    league VARCHAR,
    match_date DATE,
    player_name VARCHAR NOT NULL,
    team VARCHAR NOT NULL,
    opponent VARCHAR,
    is_home BOOLEAN,
    minutes INTEGER DEFAULT 0,
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    xg DOUBLE DEFAULT 0,
    xa DOUBLE DEFAULT 0,
    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,
    key_passes INTEGER DEFAULT 0,
    tackles INTEGER DEFAULT 0,
    interceptions INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gold.nba_games (
    game_id VARCHAR PRIMARY KEY,
    season VARCHAR,
    game_date DATE,
    home_team_id INTEGER NOT NULL,
    home_team VARCHAR NOT NULL,
    away_team_id INTEGER NOT NULL,
    away_team VARCHAR NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    home_win BOOLEAN,
    total_points INTEGER
);

CREATE TABLE IF NOT EXISTS gold.nba_player_games (
    game_id VARCHAR,
    season VARCHAR,
    game_date DATE,
    player_id INTEGER NOT NULL,
    player_name VARCHAR NOT NULL,
    team_id INTEGER NOT NULL,
    team VARCHAR NOT NULL,
    is_home BOOLEAN,
    minutes DOUBLE DEFAULT 0,
    points INTEGER DEFAULT 0,
    rebounds INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    steals INTEGER DEFAULT 0,
    blocks INTEGER DEFAULT 0,
    turnovers INTEGER DEFAULT 0,
    field_goal_pct DOUBLE,
    three_point_pct DOUBLE,
    free_throw_pct DOUBLE,
    plus_minus DOUBLE DEFAULT 0
);
"""

VIEW_DDL = """
-- Team form overview
CREATE OR REPLACE VIEW gold.v_soccer_recent_form AS
SELECT home_team AS team, season, league,
    COUNT(*) AS matches,
    SUM(CASE WHEN result = 'H' THEN 3 WHEN result = 'D' THEN 1 ELSE 0 END) AS home_points,
    SUM(home_goals) AS goals_scored,
    SUM(away_goals) AS goals_conceded
FROM gold.soccer_matches
GROUP BY home_team, season, league;

-- NBA team summary
CREATE OR REPLACE VIEW gold.v_nba_team_summary AS
SELECT home_team AS team, season,
    COUNT(*) AS games,
    SUM(CASE WHEN home_win THEN 1 ELSE 0 END) AS home_wins,
    ROUND(AVG(home_score), 1) AS avg_home_score,
    ROUND(AVG(away_score), 1) AS avg_opponent_score
FROM gold.nba_games
GROUP BY home_team, season;
"""


def init_schema(loader: DuckDBLoader | None = None) -> None:
    """Initialize the DuckDB schema with all tables and views."""
    loader = loader or DuckDBLoader()
    log.info("initializing_duckdb_schema")
    conn = loader.get_connection()
    try:
        conn.execute(SCHEMA_DDL)
        conn.execute(VIEW_DDL)
        log.info("duckdb_schema_initialized")
    finally:
        conn.close()


def refresh_views(loader: DuckDBLoader | None = None) -> None:
    """Refresh all views (re-create)."""
    loader = loader or DuckDBLoader()
    log.info("refreshing_views")
    conn = loader.get_connection()
    try:
        conn.execute(VIEW_DDL)
        log.info("views_refreshed")
    finally:
        conn.close()
