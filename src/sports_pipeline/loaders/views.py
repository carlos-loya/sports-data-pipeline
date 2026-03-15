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

CREATE TABLE IF NOT EXISTS gold.nfl_games (
    game_id VARCHAR PRIMARY KEY,
    season INTEGER,
    week INTEGER,
    game_date DATE,
    home_team VARCHAR NOT NULL,
    away_team VARCHAR NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    overtime BOOLEAN,
    game_type VARCHAR,
    stadium VARCHAR,
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

STANDINGS_DDL = """
-- Soccer league standings: cumulative season record per team per league
CREATE OR REPLACE VIEW gold.v_soccer_league_standings AS
WITH team_matches AS (
    SELECT home_team AS team, league, season,
        1 AS played,
        CASE WHEN result = 'H' THEN 1 ELSE 0 END AS win,
        CASE WHEN result = 'D' THEN 1 ELSE 0 END AS draw,
        CASE WHEN result = 'A' THEN 1 ELSE 0 END AS loss,
        home_goals AS gf, away_goals AS ga,
        COALESCE(home_xg, 0) AS xgf, COALESCE(away_xg, 0) AS xga
    FROM gold.soccer_matches
    WHERE home_goals IS NOT NULL
    UNION ALL
    SELECT away_team AS team, league, season,
        1 AS played,
        CASE WHEN result = 'A' THEN 1 ELSE 0 END AS win,
        CASE WHEN result = 'D' THEN 1 ELSE 0 END AS draw,
        CASE WHEN result = 'H' THEN 1 ELSE 0 END AS loss,
        away_goals AS gf, home_goals AS ga,
        COALESCE(away_xg, 0) AS xgf, COALESCE(home_xg, 0) AS xga
    FROM gold.soccer_matches
    WHERE home_goals IS NOT NULL
)
SELECT
    league,
    season,
    ROW_NUMBER() OVER (
        PARTITION BY league, season
        ORDER BY SUM(win) * 3 + SUM(draw) DESC, SUM(gf) - SUM(ga) DESC, SUM(gf) DESC
    ) AS rank,
    team,
    SUM(played) AS matches_played,
    SUM(win) AS wins,
    SUM(draw) AS draws,
    SUM(loss) AS losses,
    SUM(gf) AS goals_for,
    SUM(ga) AS goals_against,
    SUM(gf) - SUM(ga) AS goal_difference,
    SUM(win) * 3 + SUM(draw) AS points,
    ROUND(SUM(xgf), 2) AS xg_for,
    ROUND(SUM(xga), 2) AS xg_against,
    ROUND(SUM(xgf) - SUM(xga), 2) AS xg_difference
FROM team_matches
GROUP BY league, season, team;

-- NBA standings: cumulative season record per team
CREATE OR REPLACE VIEW gold.v_nba_standings AS
WITH team_games AS (
    SELECT home_team AS team, home_team_id AS team_id, season,
        1 AS played,
        CASE WHEN home_win THEN 1 ELSE 0 END AS win,
        1 AS is_home,
        home_score AS pts_scored, away_score AS pts_allowed
    FROM gold.nba_games
    WHERE home_score IS NOT NULL
    UNION ALL
    SELECT away_team AS team, away_team_id AS team_id, season,
        1 AS played,
        CASE WHEN NOT home_win THEN 1 ELSE 0 END AS win,
        0 AS is_home,
        away_score AS pts_scored, home_score AS pts_allowed
    FROM gold.nba_games
    WHERE home_score IS NOT NULL
)
SELECT
    season,
    ROW_NUMBER() OVER (
        PARTITION BY season
        ORDER BY SUM(win)::DOUBLE / NULLIF(SUM(played), 0) DESC
    ) AS rank,
    team,
    team_id,
    SUM(played) AS games_played,
    SUM(win) AS wins,
    SUM(played) - SUM(win) AS losses,
    ROUND(SUM(win)::DOUBLE / NULLIF(SUM(played), 0), 3) AS win_pct,
    SUM(CASE WHEN is_home = 1 THEN win ELSE 0 END) AS home_wins,
    SUM(CASE WHEN is_home = 1 THEN 1 - win ELSE 0 END) AS home_losses,
    SUM(CASE WHEN is_home = 0 THEN win ELSE 0 END) AS away_wins,
    SUM(CASE WHEN is_home = 0 THEN 1 - win ELSE 0 END) AS away_losses,
    ROUND(AVG(pts_scored), 1) AS points_per_game,
    ROUND(AVG(pts_allowed), 1) AS points_allowed_per_game,
    ROUND(AVG(pts_scored) - AVG(pts_allowed), 1) AS point_differential
FROM team_games
GROUP BY season, team, team_id;

-- Soccer home/away splits per team per league per season
CREATE OR REPLACE VIEW gold.v_soccer_home_away_splits AS
WITH home AS (
    SELECT home_team AS team, league, season,
        COUNT(*) AS matches,
        SUM(CASE WHEN result = 'H' THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) AS draws,
        SUM(CASE WHEN result = 'A' THEN 1 ELSE 0 END) AS losses,
        SUM(home_goals) AS goals_for,
        SUM(away_goals) AS goals_against,
        ROUND(SUM(COALESCE(home_xg, 0)), 2) AS xg_for,
        ROUND(SUM(COALESCE(away_xg, 0)), 2) AS xg_against
    FROM gold.soccer_matches
    WHERE home_goals IS NOT NULL
    GROUP BY home_team, league, season
),
away AS (
    SELECT away_team AS team, league, season,
        COUNT(*) AS matches,
        SUM(CASE WHEN result = 'A' THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) AS draws,
        SUM(CASE WHEN result = 'H' THEN 1 ELSE 0 END) AS losses,
        SUM(away_goals) AS goals_for,
        SUM(home_goals) AS goals_against,
        ROUND(SUM(COALESCE(away_xg, 0)), 2) AS xg_for,
        ROUND(SUM(COALESCE(home_xg, 0)), 2) AS xg_against
    FROM gold.soccer_matches
    WHERE home_goals IS NOT NULL
    GROUP BY away_team, league, season
)
SELECT
    COALESCE(h.team, a.team) AS team,
    COALESCE(h.league, a.league) AS league,
    COALESCE(h.season, a.season) AS season,
    COALESCE(h.matches, 0) AS home_matches,
    COALESCE(h.wins, 0) AS home_wins,
    COALESCE(h.draws, 0) AS home_draws,
    COALESCE(h.losses, 0) AS home_losses,
    COALESCE(h.goals_for, 0) AS home_goals_for,
    COALESCE(h.goals_against, 0) AS home_goals_against,
    COALESCE(h.xg_for, 0) AS home_xg_for,
    COALESCE(h.xg_against, 0) AS home_xg_against,
    COALESCE(a.matches, 0) AS away_matches,
    COALESCE(a.wins, 0) AS away_wins,
    COALESCE(a.draws, 0) AS away_draws,
    COALESCE(a.losses, 0) AS away_losses,
    COALESCE(a.goals_for, 0) AS away_goals_for,
    COALESCE(a.goals_against, 0) AS away_goals_against,
    COALESCE(a.xg_for, 0) AS away_xg_for,
    COALESCE(a.xg_against, 0) AS away_xg_against
FROM home h
FULL OUTER JOIN away a
    ON h.team = a.team AND h.league = a.league AND h.season = a.season;

-- NBA home/away splits per team per season
CREATE OR REPLACE VIEW gold.v_nba_home_away_splits AS
WITH home AS (
    SELECT home_team AS team, home_team_id AS team_id, season,
        COUNT(*) AS games,
        SUM(CASE WHEN home_win THEN 1 ELSE 0 END) AS wins,
        COUNT(*) - SUM(CASE WHEN home_win THEN 1 ELSE 0 END) AS losses,
        ROUND(AVG(home_score), 1) AS avg_points_scored,
        ROUND(AVG(away_score), 1) AS avg_points_allowed
    FROM gold.nba_games
    WHERE home_score IS NOT NULL
    GROUP BY home_team, home_team_id, season
),
away AS (
    SELECT away_team AS team, away_team_id AS team_id, season,
        COUNT(*) AS games,
        SUM(CASE WHEN NOT home_win THEN 1 ELSE 0 END) AS wins,
        COUNT(*) - SUM(CASE WHEN NOT home_win THEN 1 ELSE 0 END) AS losses,
        ROUND(AVG(away_score), 1) AS avg_points_scored,
        ROUND(AVG(home_score), 1) AS avg_points_allowed
    FROM gold.nba_games
    WHERE home_score IS NOT NULL
    GROUP BY away_team, away_team_id, season
)
SELECT
    COALESCE(h.team, a.team) AS team,
    COALESCE(h.team_id, a.team_id) AS team_id,
    COALESCE(h.season, a.season) AS season,
    COALESCE(h.games, 0) AS home_games,
    COALESCE(h.wins, 0) AS home_wins,
    COALESCE(h.losses, 0) AS home_losses,
    COALESCE(h.avg_points_scored, 0) AS home_avg_points_scored,
    COALESCE(h.avg_points_allowed, 0) AS home_avg_points_allowed,
    COALESCE(a.games, 0) AS away_games,
    COALESCE(a.wins, 0) AS away_wins,
    COALESCE(a.losses, 0) AS away_losses,
    COALESCE(a.avg_points_scored, 0) AS away_avg_points_scored,
    COALESCE(a.avg_points_allowed, 0) AS away_avg_points_allowed
FROM home h
FULL OUTER JOIN away a
    ON h.team = a.team AND h.team_id = a.team_id AND h.season = a.season;

-- NFL standings: cumulative season record per team
-- Uses nfl_games table which is created dynamically by the DAG loader.
-- The view silently returns empty if the table does not exist yet.
CREATE OR REPLACE VIEW gold.v_nfl_standings AS
WITH team_games AS (
    SELECT home_team AS team, season,
        1 AS played,
        CASE WHEN home_win THEN 1 ELSE 0 END AS win,
        CASE WHEN home_score = away_score THEN 1 ELSE 0 END AS tie,
        1 AS is_home,
        home_score AS pts_scored, away_score AS pts_allowed
    FROM gold.nfl_games
    WHERE home_score IS NOT NULL
    UNION ALL
    SELECT away_team AS team, season,
        1 AS played,
        CASE WHEN NOT home_win AND home_score != away_score THEN 1 ELSE 0 END AS win,
        CASE WHEN home_score = away_score THEN 1 ELSE 0 END AS tie,
        0 AS is_home,
        away_score AS pts_scored, home_score AS pts_allowed
    FROM gold.nfl_games
    WHERE home_score IS NOT NULL
)
SELECT
    season,
    ROW_NUMBER() OVER (
        PARTITION BY season
        ORDER BY SUM(win)::DOUBLE / NULLIF(SUM(played), 0) DESC,
                 SUM(pts_scored) - SUM(pts_allowed) DESC
    ) AS rank,
    team,
    SUM(played) AS games_played,
    SUM(win) AS wins,
    SUM(played) - SUM(win) - SUM(tie) AS losses,
    SUM(tie) AS ties,
    ROUND(SUM(win)::DOUBLE / NULLIF(SUM(played), 0), 3) AS win_pct,
    SUM(CASE WHEN is_home = 1 THEN win ELSE 0 END) AS home_wins,
    SUM(CASE WHEN is_home = 1 THEN 1 - win - tie ELSE 0 END) AS home_losses,
    SUM(CASE WHEN is_home = 0 THEN win ELSE 0 END) AS away_wins,
    SUM(CASE WHEN is_home = 0 THEN 1 - win - tie ELSE 0 END) AS away_losses,
    SUM(pts_scored) AS points_for,
    SUM(pts_allowed) AS points_against,
    SUM(pts_scored) - SUM(pts_allowed) AS point_differential,
    ROUND(AVG(pts_scored), 1) AS points_per_game,
    ROUND(AVG(pts_allowed), 1) AS points_allowed_per_game
FROM team_games
GROUP BY season, team;
"""


def init_schema(loader: DuckDBLoader | None = None) -> None:
    """Initialize the DuckDB schema with all tables and views."""
    loader = loader or DuckDBLoader()
    log.info("initializing_duckdb_schema")
    conn = loader.get_connection()
    try:
        conn.execute(SCHEMA_DDL)
        conn.execute(VIEW_DDL)
        conn.execute(STANDINGS_DDL)
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
        conn.execute(STANDINGS_DDL)
        log.info("views_refreshed")
    finally:
        conn.close()
