"""Gold layer feature builder: rolling form, H2H, team splits."""

from __future__ import annotations

import pandas as pd

from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


class GoldBuilder:
    """Build gold-layer analytics tables from silver data."""

    def __init__(self, loader: DuckDBLoader | None = None) -> None:
        self.loader = loader or DuckDBLoader()

    def build_soccer_team_form(self, n_matches: int = 5) -> pd.DataFrame:
        """Build rolling team form table for soccer.

        Computes last N matches' results, goals, xG for each team.
        """
        log.info("building_soccer_team_form", n_matches=n_matches)

        sql = f"""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY team, league
                    ORDER BY match_date DESC
                ) AS rn
            FROM (
                SELECT home_team AS team, league, match_date,
                    CASE WHEN result = 'H' THEN 'W'
                         WHEN result = 'D' THEN 'D'
                         ELSE 'L' END AS outcome,
                    home_goals AS goals_scored,
                    away_goals AS goals_conceded,
                    home_xg AS xg_for,
                    away_xg AS xg_against
                FROM gold.soccer_matches
                UNION ALL
                SELECT away_team AS team, league, match_date,
                    CASE WHEN result = 'A' THEN 'W'
                         WHEN result = 'D' THEN 'D'
                         ELSE 'L' END AS outcome,
                    away_goals AS goals_scored,
                    home_goals AS goals_conceded,
                    away_xg AS xg_for,
                    home_xg AS xg_against
                FROM gold.soccer_matches
            )
        )
        SELECT
            team,
            league,
            MAX(match_date) AS as_of_date,
            {n_matches} AS last_n_matches,
            SUM(CASE WHEN outcome = 'W' THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN outcome = 'D' THEN 1 ELSE 0 END) AS draws,
            SUM(CASE WHEN outcome = 'L' THEN 1 ELSE 0 END) AS losses,
            SUM(goals_scored) AS goals_scored,
            SUM(goals_conceded) AS goals_conceded,
            ROUND(SUM(COALESCE(xg_for, 0)), 2) AS xg_for,
            ROUND(SUM(COALESCE(xg_against, 0)), 2) AS xg_against,
            SUM(CASE WHEN outcome = 'W' THEN 3
                     WHEN outcome = 'D' THEN 1
                     ELSE 0 END) AS points,
            STRING_AGG(outcome, '' ORDER BY match_date DESC) AS form_string
        FROM ranked
        WHERE rn <= {n_matches}
        GROUP BY team, league
        """

        try:
            result = self.loader.query(sql)
            if not result.empty:
                self.loader.load_dataframe(result, "soccer_team_form", mode="replace")
            log.info("built_soccer_team_form", rows=len(result))
            return result
        except Exception as e:
            log.warning("soccer_team_form_failed", error=str(e))
            return pd.DataFrame()

    def build_soccer_h2h(self) -> pd.DataFrame:
        """Build head-to-head records between soccer teams."""
        log.info("building_soccer_h2h")

        sql = """
        SELECT
            LEAST(home_team, away_team) AS team_a,
            GREATEST(home_team, away_team) AS team_b,
            league,
            COUNT(*) AS total_matches,
            SUM(CASE WHEN
                (LEAST(home_team, away_team) = home_team AND result = 'H') OR
                (LEAST(home_team, away_team) = away_team AND result = 'A')
                THEN 1 ELSE 0 END) AS team_a_wins,
            SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) AS draws,
            SUM(CASE WHEN
                (GREATEST(home_team, away_team) = home_team AND result = 'H') OR
                (GREATEST(home_team, away_team) = away_team AND result = 'A')
                THEN 1 ELSE 0 END) AS team_b_wins,
            SUM(CASE WHEN LEAST(home_team, away_team) = home_team
                THEN home_goals ELSE away_goals END) AS team_a_goals,
            SUM(CASE WHEN GREATEST(home_team, away_team) = home_team
                THEN home_goals ELSE away_goals END) AS team_b_goals
        FROM gold.soccer_matches
        WHERE home_goals IS NOT NULL
        GROUP BY LEAST(home_team, away_team), GREATEST(home_team, away_team), league
        """

        try:
            result = self.loader.query(sql)
            if not result.empty:
                self.loader.load_dataframe(result, "soccer_head_to_head", mode="replace")
            log.info("built_soccer_h2h", rows=len(result))
            return result
        except Exception as e:
            log.warning("soccer_h2h_failed", error=str(e))
            return pd.DataFrame()

    def build_nba_team_form(self, n_games: int = 10) -> pd.DataFrame:
        """Build rolling team form table for NBA."""
        log.info("building_nba_team_form", n_games=n_games)

        sql = f"""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY team_id
                    ORDER BY game_date DESC
                ) AS rn
            FROM (
                SELECT home_team_id AS team_id, home_team AS team, game_date,
                    home_score AS pts_scored, away_score AS pts_allowed,
                    CASE WHEN home_win THEN 1 ELSE 0 END AS win
                FROM gold.nba_games
                WHERE home_score IS NOT NULL
                UNION ALL
                SELECT away_team_id AS team_id, away_team AS team, game_date,
                    away_score AS pts_scored, home_score AS pts_allowed,
                    CASE WHEN NOT home_win THEN 1 ELSE 0 END AS win
                FROM gold.nba_games
                WHERE home_score IS NOT NULL
            )
        )
        SELECT
            team,
            team_id,
            MAX(game_date) AS as_of_date,
            {n_games} AS last_n_games,
            SUM(win) AS wins,
            {n_games} - SUM(win) AS losses,
            ROUND(AVG(pts_scored), 1) AS avg_points_scored,
            ROUND(AVG(pts_allowed), 1) AS avg_points_allowed,
            ROUND(AVG(pts_scored - pts_allowed), 1) AS avg_point_diff
        FROM ranked
        WHERE rn <= {n_games}
        GROUP BY team, team_id
        """

        try:
            result = self.loader.query(sql)
            if not result.empty:
                self.loader.load_dataframe(result, "nba_team_form", mode="replace")
            log.info("built_nba_team_form", rows=len(result))
            return result
        except Exception as e:
            log.warning("nba_team_form_failed", error=str(e))
            return pd.DataFrame()
