"""Tests for NFL transformers."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from sports_pipeline.transformers.nfl.game_transformer import NflGameTransformer
from sports_pipeline.transformers.nfl.player_transformer import NflPlayerTransformer


def _sample_bronze_nfl_games():
    """Create sample bronze NFL game data."""
    return pd.DataFrame([
        {
            "extract_timestamp": datetime.utcnow(),
            "season": 2024,
            "week": 1,
            "game_id": "2024_01_KC_BAL",
            "gameday": "2024-09-05",
            "home_team": "BAL",
            "away_team": "KC",
            "home_score": 20,
            "away_score": 27,
            "overtime": False,
            "game_type": "REG",
            "stadium": "M&T Bank Stadium",
        },
        {
            "extract_timestamp": datetime.utcnow(),
            "season": 2024,
            "week": 1,
            "game_id": "2024_01_GB_PHI",
            "gameday": "2024-09-06",
            "home_team": "PHI",
            "away_team": "GB",
            "home_score": 34,
            "away_score": 29,
            "overtime": False,
            "game_type": "REG",
            "stadium": "Lincoln Financial Field",
        },
        {
            "extract_timestamp": datetime.utcnow(),
            "season": 2024,
            "week": 2,
            "game_id": "2024_02_KC_CIN",
            "gameday": "2024-09-15",
            "home_team": "CIN",
            "away_team": "KC",
            "home_score": 25,
            "away_score": 26,
            "overtime": True,
            "game_type": "REG",
            "stadium": "Paycor Stadium",
        },
    ])


def _sample_bronze_nfl_players():
    """Create sample bronze NFL player game data."""
    return pd.DataFrame([
        {
            "extract_timestamp": datetime.utcnow(),
            "season": 2024,
            "week": 1,
            "player_id": "00-0036442",
            "player_name": "P.Mahomes",
            "player_display_name": "Patrick Mahomes",
            "team": "KC",
            "position": "QB",
            "completions": 20,
            "passing_attempts": 28,
            "passing_yards": 291.0,
            "passing_tds": 1,
            "interceptions": 0,
            "sacks": 2,
            "carries": 3,
            "rushing_yards": 24.0,
            "rushing_tds": 0,
            "rushing_fumbles": 0,
            "receptions": 0,
            "targets": 0,
            "receiving_yards": 0.0,
            "receiving_tds": 0,
            "fantasy_points": 19.64,
        },
        {
            "extract_timestamp": datetime.utcnow(),
            "season": 2024,
            "week": 1,
            "player_id": "00-0039337",
            "player_name": "J.Hurts",
            "player_display_name": "Jalen Hurts",
            "team": "PHI",
            "position": "QB",
            "completions": 18,
            "passing_attempts": 33,
            "passing_yards": 278.0,
            "passing_tds": 2,
            "interceptions": 0,
            "sacks": 1,
            "carries": 13,
            "rushing_yards": 84.0,
            "rushing_tds": 2,
            "rushing_fumbles": 0,
            "receptions": 0,
            "targets": 0,
            "receiving_yards": 0.0,
            "receiving_tds": 0,
            "fantasy_points": 33.52,
        },
    ])


class TestNflGameTransformer:
    def test_transform_basic(self):
        df = _sample_bronze_nfl_games()
        transformer = NflGameTransformer()
        result = transformer.transform(df)

        assert not result.empty
        assert len(result) == 3
        assert "home_win" in result.columns
        assert "total_points" in result.columns

    def test_home_win_derived(self):
        df = _sample_bronze_nfl_games()
        transformer = NflGameTransformer()
        result = transformer.transform(df)

        # BAL 20 vs KC 27 => home_win = False
        bal_game = result[result["game_id"] == "2024_01_KC_BAL"].iloc[0]
        assert not bal_game["home_win"]

        # PHI 34 vs GB 29 => home_win = True
        phi_game = result[result["game_id"] == "2024_01_GB_PHI"].iloc[0]
        assert phi_game["home_win"]

    def test_total_points_derived(self):
        df = _sample_bronze_nfl_games()
        transformer = NflGameTransformer()
        result = transformer.transform(df)

        bal_game = result[result["game_id"] == "2024_01_KC_BAL"].iloc[0]
        assert bal_game["total_points"] == 47  # 20 + 27

    def test_team_names_normalized(self):
        df = _sample_bronze_nfl_games()
        transformer = NflGameTransformer()
        result = transformer.transform(df)

        # BAL should normalize to "Baltimore Ravens"
        bal_game = result[result["game_id"] == "2024_01_KC_BAL"].iloc[0]
        assert bal_game["home_team"] == "Baltimore Ravens"
        assert bal_game["away_team"] == "Kansas City Chiefs"

    def test_deduplication(self):
        df = _sample_bronze_nfl_games()
        # Duplicate a row
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        transformer = NflGameTransformer()
        result = transformer.transform(df)

        assert len(result) == 3  # Duplicate removed

    def test_transform_empty(self):
        transformer = NflGameTransformer()
        result = transformer.transform(pd.DataFrame())
        assert result.empty

    def test_null_scores_handled(self):
        df = pd.DataFrame([{
            "extract_timestamp": datetime.utcnow(),
            "season": 2024,
            "week": 1,
            "game_id": "2024_01_KC_BAL",
            "gameday": "2024-09-05",
            "home_team": "BAL",
            "away_team": "KC",
            "home_score": None,
            "away_score": None,
            "overtime": False,
            "game_type": "REG",
            "stadium": "M&T Bank Stadium",
        }])
        transformer = NflGameTransformer()
        result = transformer.transform(df)

        assert result.iloc[0]["home_win"] is None
        assert result.iloc[0]["total_points"] is None


class TestNflPlayerTransformer:
    def test_transform_basic(self):
        df = _sample_bronze_nfl_players()
        transformer = NflPlayerTransformer()
        result = transformer.transform(df)

        assert not result.empty
        assert len(result) == 2

    def test_player_name_normalized(self):
        df = _sample_bronze_nfl_players()
        transformer = NflPlayerTransformer()
        result = transformer.transform(df)

        # Should use display name, not abbreviated name
        assert result.iloc[0]["player_name"] == "Patrick Mahomes"
        assert result.iloc[1]["player_name"] == "Jalen Hurts"

    def test_team_normalized(self):
        df = _sample_bronze_nfl_players()
        transformer = NflPlayerTransformer()
        result = transformer.transform(df)

        # KC -> Kansas City Chiefs
        assert result.iloc[0]["team"] == "Kansas City Chiefs"

    def test_total_yards_derived(self):
        df = _sample_bronze_nfl_players()
        transformer = NflPlayerTransformer()
        result = transformer.transform(df)

        # Mahomes: 291 passing + 24 rushing + 0 receiving = 315
        mahomes = result[result["player_id"] == "00-0036442"].iloc[0]
        assert mahomes["total_yards"] == 315.0

    def test_total_tds_derived(self):
        df = _sample_bronze_nfl_players()
        transformer = NflPlayerTransformer()
        result = transformer.transform(df)

        # Hurts: 2 passing + 2 rushing + 0 receiving = 4
        hurts = result[result["player_id"] == "00-0039337"].iloc[0]
        assert hurts["total_tds"] == 4

    def test_deduplication(self):
        df = _sample_bronze_nfl_players()
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        transformer = NflPlayerTransformer()
        result = transformer.transform(df)

        assert len(result) == 2

    def test_transform_empty(self):
        transformer = NflPlayerTransformer()
        result = transformer.transform(pd.DataFrame())
        assert result.empty
