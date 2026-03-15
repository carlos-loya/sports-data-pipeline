"""Tests for NFL extractors."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from sports_pipeline.extractors.nfl.game_extractor import NflGameExtractor
from sports_pipeline.extractors.nfl.player_extractor import NflPlayerExtractor
from sports_pipeline.extractors.nfl.team_extractor import NflTeamExtractor


class TestNflGameExtractor:
    def test_extract_full_season(self, sample_nfl_schedule):
        client = MagicMock()
        client.get_schedules.return_value = sample_nfl_schedule
        extractor = NflGameExtractor(client=client)

        result = extractor.extract(season=2024)

        assert not result.empty
        assert len(result) == 3
        assert list(result["home_team"]) == ["BAL", "PHI", "CIN"]
        assert list(result["away_team"]) == ["KC", "GB", "KC"]
        client.get_schedules.assert_called_once_with(season=2024)

    def test_extract_single_week(self, sample_nfl_schedule):
        client = MagicMock()
        client.get_schedules.return_value = sample_nfl_schedule
        extractor = NflGameExtractor(client=client)

        result = extractor.extract(season=2024, week=1)

        assert len(result) == 2
        assert all(result["week"] == 1)

    def test_extract_empty_week(self, sample_nfl_schedule):
        client = MagicMock()
        client.get_schedules.return_value = sample_nfl_schedule
        extractor = NflGameExtractor(client=client)

        result = extractor.extract(season=2024, week=18)

        assert result.empty

    def test_extract_empty_season(self):
        client = MagicMock()
        client.get_schedules.return_value = pd.DataFrame()
        extractor = NflGameExtractor(client=client)

        result = extractor.extract(season=2024)

        assert result.empty

    def test_overtime_normalized_to_bool(self, sample_nfl_schedule):
        client = MagicMock()
        client.get_schedules.return_value = sample_nfl_schedule
        extractor = NflGameExtractor(client=client)

        result = extractor.extract(season=2024)

        # Week 1 games have no overtime, week 2 game has "OT"
        assert result.iloc[0]["overtime"] is False or not result.iloc[0]["overtime"]
        assert result.iloc[2]["overtime"] is True or result.iloc[2]["overtime"]


class TestNflPlayerExtractor:
    def test_extract_full_season(self, sample_nfl_player_stats):
        client = MagicMock()
        client.get_player_stats.return_value = sample_nfl_player_stats
        extractor = NflPlayerExtractor(client=client)

        result = extractor.extract(season=2024)

        assert not result.empty
        assert len(result) == 3
        assert result.iloc[0]["player_display_name"] == "Patrick Mahomes"
        assert result.iloc[0]["passing_yards"] == 291.0
        client.get_player_stats.assert_called_once_with(season=2024, summary_level="week")

    def test_extract_single_week(self, sample_nfl_player_stats):
        client = MagicMock()
        client.get_player_stats.return_value = sample_nfl_player_stats
        extractor = NflPlayerExtractor(client=client)

        result = extractor.extract(season=2024, week=1)

        assert len(result) == 2
        assert all(result["week"] == 1)

    def test_extract_empty_week(self, sample_nfl_player_stats):
        client = MagicMock()
        client.get_player_stats.return_value = sample_nfl_player_stats
        extractor = NflPlayerExtractor(client=client)

        result = extractor.extract(season=2024, week=18)

        assert result.empty

    def test_extract_empty_season(self):
        client = MagicMock()
        client.get_player_stats.return_value = pd.DataFrame()
        extractor = NflPlayerExtractor(client=client)

        result = extractor.extract(season=2024)

        assert result.empty

    def test_player_stats_columns(self, sample_nfl_player_stats):
        client = MagicMock()
        client.get_player_stats.return_value = sample_nfl_player_stats
        extractor = NflPlayerExtractor(client=client)

        result = extractor.extract(season=2024)

        expected_cols = [
            "extract_timestamp",
            "season",
            "week",
            "player_id",
            "player_name",
            "player_display_name",
            "team",
            "position",
            "completions",
            "passing_attempts",
            "passing_yards",
            "passing_tds",
            "interceptions",
            "sacks",
            "passing_air_yards",
            "passing_yards_after_catch",
            "passing_2pt_conversions",
            "carries",
            "rushing_yards",
            "rushing_tds",
            "rushing_fumbles",
            "rushing_2pt_conversions",
            "receptions",
            "targets",
            "receiving_yards",
            "receiving_tds",
            "receiving_air_yards",
            "receiving_yards_after_catch",
            "receiving_fumbles",
            "receiving_2pt_conversions",
            "fantasy_points",
        ]
        for col in expected_cols:
            assert col in result.columns, f"Missing column: {col}"


class TestNflTeamExtractor:
    def test_extract_team_stats(self, sample_nfl_schedule):
        client = MagicMock()
        client.get_schedules.return_value = sample_nfl_schedule
        extractor = NflTeamExtractor(client=client)

        result = extractor.extract(season=2024)

        assert not result.empty
        # 5 unique teams in fixture: BAL, KC, PHI, GB, CIN
        assert len(result) == 5
        client.get_schedules.assert_called_once_with(season=2024)

    def test_team_stats_columns(self, sample_nfl_schedule):
        client = MagicMock()
        client.get_schedules.return_value = sample_nfl_schedule
        extractor = NflTeamExtractor(client=client)

        result = extractor.extract(season=2024)

        expected_cols = [
            "extract_timestamp",
            "season",
            "team",
            "games_played",
            "wins",
            "losses",
            "ties",
            "points_for",
            "points_against",
            "point_differential",
            "home_wins",
            "home_losses",
            "away_wins",
            "away_losses",
        ]
        for col in expected_cols:
            assert col in result.columns, f"Missing column: {col}"

    def test_wins_losses_correct(self, sample_nfl_schedule):
        client = MagicMock()
        client.get_schedules.return_value = sample_nfl_schedule
        extractor = NflTeamExtractor(client=client)

        result = extractor.extract(season=2024)

        # KC: away W at BAL (27-20), away W at CIN (26-25) => 2 wins, 0 losses
        kc = result[result["team"] == "KC"].iloc[0]
        assert kc["wins"] == 2
        assert kc["losses"] == 0
        assert kc["away_wins"] == 2
        assert kc["games_played"] == 2

        # BAL: home L to KC (20-27) => 0 wins, 1 loss
        bal = result[result["team"] == "BAL"].iloc[0]
        assert bal["wins"] == 0
        assert bal["losses"] == 1
        assert bal["home_losses"] == 1

    def test_point_differential(self, sample_nfl_schedule):
        client = MagicMock()
        client.get_schedules.return_value = sample_nfl_schedule
        extractor = NflTeamExtractor(client=client)

        result = extractor.extract(season=2024)

        # PHI: home W (34-29) => PF=34, PA=29, diff=+5
        phi = result[result["team"] == "PHI"].iloc[0]
        assert phi["points_for"] == 34
        assert phi["points_against"] == 29
        assert phi["point_differential"] == 5

    def test_extract_empty_season(self):
        client = MagicMock()
        client.get_schedules.return_value = pd.DataFrame()
        extractor = NflTeamExtractor(client=client)

        result = extractor.extract(season=2024)

        assert result.empty

    def test_extract_no_completed_games(self):
        """Games without scores should be excluded."""
        client = MagicMock()
        client.get_schedules.return_value = pd.DataFrame(
            [
                {
                    "game_id": "2024_01_KC_BAL",
                    "season": 2024,
                    "week": 1,
                    "home_team": "BAL",
                    "away_team": "KC",
                    "home_score": None,
                    "away_score": None,
                }
            ]
        )
        extractor = NflTeamExtractor(client=client)

        result = extractor.extract(season=2024)

        assert result.empty
