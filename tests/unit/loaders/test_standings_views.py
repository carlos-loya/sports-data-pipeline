"""Tests for league standings and home/away split views."""

from __future__ import annotations

import pandas as pd
import pytest

from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
from sports_pipeline.loaders.views import init_schema


@pytest.fixture
def db(tmp_path):
    """Create a temporary DuckDB with schema and sample data."""
    loader = DuckDBLoader(db_path=tmp_path / "test.duckdb")
    init_schema(loader)

    # Insert sample soccer matches
    soccer_df = pd.DataFrame(
        [
            {
                "match_id": "m1",
                "season": "2024-2025",
                "league": "Premier League",
                "match_date": "2024-09-14",
                "home_team": "Arsenal",
                "away_team": "Wolves",
                "home_goals": 2,
                "away_goals": 0,
                "home_xg": 2.1,
                "away_xg": 0.5,
                "result": "H",
                "venue": "Emirates Stadium",
            },
            {
                "match_id": "m2",
                "season": "2024-2025",
                "league": "Premier League",
                "match_date": "2024-09-21",
                "home_team": "Wolves",
                "away_team": "Arsenal",
                "home_goals": 1,
                "away_goals": 1,
                "home_xg": 0.8,
                "away_xg": 1.3,
                "result": "D",
                "venue": "Molineux",
            },
            {
                "match_id": "m3",
                "season": "2024-2025",
                "league": "Premier League",
                "match_date": "2024-09-28",
                "home_team": "Arsenal",
                "away_team": "Man City",
                "home_goals": 0,
                "away_goals": 1,
                "home_xg": 1.0,
                "away_xg": 1.2,
                "result": "A",
                "venue": "Emirates Stadium",
            },
        ]
    )
    loader.load_dataframe(soccer_df, "soccer_matches", mode="replace")

    # Insert sample NBA games
    nba_df = pd.DataFrame(
        [
            {
                "game_id": "g1",
                "season": "2024-25",
                "game_date": "2024-10-22",
                "home_team_id": 1,
                "home_team": "Lakers",
                "away_team_id": 2,
                "away_team": "Celtics",
                "home_score": 110,
                "away_score": 105,
                "home_win": True,
                "total_points": 215,
            },
            {
                "game_id": "g2",
                "season": "2024-25",
                "game_date": "2024-10-24",
                "home_team_id": 2,
                "home_team": "Celtics",
                "away_team_id": 1,
                "away_team": "Lakers",
                "home_score": 120,
                "away_score": 100,
                "home_win": True,
                "total_points": 220,
            },
            {
                "game_id": "g3",
                "season": "2024-25",
                "game_date": "2024-10-26",
                "home_team_id": 1,
                "home_team": "Lakers",
                "away_team_id": 3,
                "away_team": "Warriors",
                "home_score": 115,
                "away_score": 108,
                "home_win": True,
                "total_points": 223,
            },
        ]
    )
    loader.load_dataframe(nba_df, "nba_games", mode="replace")

    # Re-create views after data is loaded
    from sports_pipeline.loaders.views import refresh_views

    refresh_views(loader)

    return loader


class TestSoccerLeagueStandings:
    def test_standings_has_all_teams(self, db):
        result = db.query("SELECT * FROM gold.v_soccer_league_standings ORDER BY rank")
        assert len(result) == 3
        teams = set(result["team"])
        assert teams == {"Arsenal", "Wolves", "Man City"}

    def test_standings_points_correct(self, db):
        result = db.query("SELECT * FROM gold.v_soccer_league_standings ORDER BY rank")
        standings = result.set_index("team")

        # Arsenal: 1W 1D 1L = 4 pts, GD = 2-0 + 1-1 + 0-1 = +1
        assert standings.loc["Arsenal", "points"] == 4
        assert standings.loc["Arsenal", "wins"] == 1
        assert standings.loc["Arsenal", "draws"] == 1
        assert standings.loc["Arsenal", "losses"] == 1
        assert standings.loc["Arsenal", "goal_difference"] == 1

        # Man City: 1W 0D 0L = 3 pts
        assert standings.loc["Man City", "points"] == 3

        # Wolves: 0W 1D 1L = 1 pt
        assert standings.loc["Wolves", "points"] == 1

    def test_standings_ranked_by_points(self, db):
        result = db.query(
            "SELECT team, rank, points FROM gold.v_soccer_league_standings ORDER BY rank"
        )
        # Arsenal 4pts > Man City 3pts > Wolves 1pt
        assert result.iloc[0]["team"] == "Arsenal"
        assert result.iloc[1]["team"] == "Man City"
        assert result.iloc[2]["team"] == "Wolves"

    def test_standings_xg_columns(self, db):
        result = db.query("SELECT * FROM gold.v_soccer_league_standings ORDER BY rank")
        assert "xg_for" in result.columns
        assert "xg_against" in result.columns
        assert "xg_difference" in result.columns


class TestNbaStandings:
    def test_standings_has_all_teams(self, db):
        result = db.query("SELECT * FROM gold.v_nba_standings ORDER BY rank")
        assert len(result) == 3
        teams = set(result["team"])
        assert teams == {"Lakers", "Celtics", "Warriors"}

    def test_standings_record_correct(self, db):
        result = db.query("SELECT * FROM gold.v_nba_standings ORDER BY rank")
        standings = result.set_index("team")

        # Lakers: W vs Celtics, L vs Celtics (away), W vs Warriors = 2-1
        assert standings.loc["Lakers", "wins"] == 2
        assert standings.loc["Lakers", "losses"] == 1

        # Celtics: L vs Lakers (away), W vs Lakers (home) = 1-1
        assert standings.loc["Celtics", "wins"] == 1
        assert standings.loc["Celtics", "losses"] == 1

        # Warriors: L vs Lakers (away) = 0-1
        assert standings.loc["Warriors", "wins"] == 0
        assert standings.loc["Warriors", "losses"] == 1

    def test_standings_win_pct(self, db):
        result = db.query("SELECT * FROM gold.v_nba_standings ORDER BY rank")
        standings = result.set_index("team")

        assert standings.loc["Lakers", "win_pct"] == pytest.approx(0.667, abs=0.001)
        assert standings.loc["Celtics", "win_pct"] == pytest.approx(0.5, abs=0.001)
        assert standings.loc["Warriors", "win_pct"] == pytest.approx(0.0, abs=0.001)

    def test_standings_ranked_by_win_pct(self, db):
        result = db.query("SELECT team, rank, win_pct FROM gold.v_nba_standings ORDER BY rank")
        assert result.iloc[0]["team"] == "Lakers"
        assert result.iloc[1]["team"] == "Celtics"
        assert result.iloc[2]["team"] == "Warriors"

    def test_standings_home_away_record(self, db):
        result = db.query("SELECT * FROM gold.v_nba_standings ORDER BY rank")
        standings = result.set_index("team")

        # Lakers: 2 home wins, 0 home losses, 0 away wins, 1 away loss
        assert standings.loc["Lakers", "home_wins"] == 2
        assert standings.loc["Lakers", "home_losses"] == 0
        assert standings.loc["Lakers", "away_wins"] == 0
        assert standings.loc["Lakers", "away_losses"] == 1


class TestSoccerHomeAwaySplits:
    def test_splits_has_all_teams(self, db):
        result = db.query("SELECT * FROM gold.v_soccer_home_away_splits")
        assert len(result) == 3

    def test_splits_home_away_correct(self, db):
        result = db.query("SELECT * FROM gold.v_soccer_home_away_splits")
        splits = result.set_index("team")

        # Arsenal: 2 home matches (1W, 1L), 1 away match (1D)
        assert splits.loc["Arsenal", "home_matches"] == 2
        assert splits.loc["Arsenal", "home_wins"] == 1
        assert splits.loc["Arsenal", "home_losses"] == 1
        assert splits.loc["Arsenal", "away_matches"] == 1
        assert splits.loc["Arsenal", "away_draws"] == 1


class TestNbaHomeAwaySplits:
    def test_splits_has_all_teams(self, db):
        result = db.query("SELECT * FROM gold.v_nba_home_away_splits")
        assert len(result) == 3

    def test_splits_correct(self, db):
        result = db.query("SELECT * FROM gold.v_nba_home_away_splits")
        splits = result.set_index("team")

        # Lakers: 2 home games (2W), 1 away game (0W 1L)
        assert splits.loc["Lakers", "home_games"] == 2
        assert splits.loc["Lakers", "home_wins"] == 2
        assert splits.loc["Lakers", "away_games"] == 1
        assert splits.loc["Lakers", "away_losses"] == 1
