"""Tests for NBA extractors."""

from __future__ import annotations

from unittest.mock import MagicMock

from sports_pipeline.extractors.nba.game_extractor import NbaGameExtractor
from sports_pipeline.extractors.nba.player_extractor import NbaPlayerExtractor
from sports_pipeline.extractors.nba.team_extractor import NbaTeamExtractor


class TestNbaGameExtractor:
    def test_extract_pairs_home_away(self, sample_nba_game_log):
        client = MagicMock()
        client.get_league_game_log.return_value = sample_nba_game_log
        extractor = NbaGameExtractor(client=client)

        result = extractor.extract(season="2024-25")

        assert not result.empty
        assert len(result) == 1  # One game from two team entries
        assert result.iloc[0]["home_team_name"] == "Los Angeles Lakers"
        assert result.iloc[0]["away_team_name"] == "Boston Celtics"
        assert result.iloc[0]["home_score"] == 110
        assert result.iloc[0]["away_score"] == 105

    def test_extract_empty_response(self):
        client = MagicMock()
        client.get_league_game_log.return_value = []
        extractor = NbaGameExtractor(client=client)

        result = extractor.extract(season="2024-25")
        assert result.empty


class TestNbaPlayerExtractor:
    def test_extract_player_stats(self, sample_nba_player_log):
        client = MagicMock()
        client.get_league_player_game_log.return_value = sample_nba_player_log
        extractor = NbaPlayerExtractor(client=client)

        result = extractor.extract(season="2024-25")

        assert not result.empty
        assert result.iloc[0]["player_name"] == "LeBron James"
        assert result.iloc[0]["team_name"] == "Los Angeles Lakers"
        assert result.iloc[0]["points"] == 28
        assert result.iloc[0]["rebounds"] == 8
        assert result.iloc[0]["assists"] == 10

    def test_extract_includes_team_id(self, sample_nba_player_log):
        client = MagicMock()
        client.get_league_player_game_log.return_value = sample_nba_player_log
        extractor = NbaPlayerExtractor(client=client)

        result = extractor.extract(season="2024-25")

        assert "team_id" in result.columns
        assert result.iloc[0]["team_id"] == 1610612747

    def test_extract_derives_is_home_from_matchup(self, sample_nba_player_log):
        client = MagicMock()
        client.get_league_player_game_log.return_value = sample_nba_player_log
        extractor = NbaPlayerExtractor(client=client)

        result = extractor.extract(season="2024-25")

        assert result.iloc[0]["is_home"] == True  # noqa: E712

    def test_extract_away_game(self):
        client = MagicMock()
        client.get_league_player_game_log.return_value = [
            {
                "SEASON_ID": "22024",
                "PLAYER_ID": 2544,
                "PLAYER_NAME": "LeBron James",
                "TEAM_ID": 1610612747,
                "TEAM_ABBREVIATION": "LAL",
                "TEAM_NAME": "Los Angeles Lakers",
                "GAME_ID": "0022400002",
                "GAME_DATE": "2024-10-24",
                "MATCHUP": "LAL @ BOS",
                "WL": "L",
                "MIN": 34.0,
                "PTS": 22,
                "REB": 6,
                "AST": 7,
                "STL": 1,
                "BLK": 0,
                "TOV": 4,
                "FGM": 8,
                "FGA": 18,
                "FG3M": 2,
                "FG3A": 6,
                "FTM": 4,
                "FTA": 4,
                "PLUS_MINUS": -8.0,
            },
        ]
        extractor = NbaPlayerExtractor(client=client)

        result = extractor.extract(season="2024-25")

        assert result.iloc[0]["is_home"] == False  # noqa: E712

    def test_extract_columns_present(self, sample_nba_player_log):
        client = MagicMock()
        client.get_league_player_game_log.return_value = sample_nba_player_log
        extractor = NbaPlayerExtractor(client=client)

        result = extractor.extract(season="2024-25")

        expected_cols = [
            "extract_timestamp",
            "season",
            "game_id",
            "game_date",
            "player_id",
            "player_name",
            "team_id",
            "team_name",
            "is_home",
            "minutes",
            "points",
            "rebounds",
            "assists",
            "steals",
            "blocks",
            "turnovers",
            "field_goals_made",
            "field_goals_attempted",
            "three_pointers_made",
            "three_pointers_attempted",
            "free_throws_made",
            "free_throws_attempted",
            "plus_minus",
        ]
        for col in expected_cols:
            assert col in result.columns, f"Missing column: {col}"

    def test_extract_empty_response(self):
        client = MagicMock()
        client.get_league_player_game_log.return_value = []
        extractor = NbaPlayerExtractor(client=client)

        result = extractor.extract(season="2024-25")
        assert result.empty


class TestNbaTeamExtractor:
    def test_extract_team_stats(self, sample_nba_team_metrics):
        client = MagicMock()
        client.get_team_estimated_metrics.return_value = sample_nba_team_metrics
        extractor = NbaTeamExtractor(client=client)

        result = extractor.extract(season="2024-25")

        assert len(result) == 2
        lakers = result[result["team_name"] == "Los Angeles Lakers"].iloc[0]
        assert lakers["wins"] == 12
        assert lakers["offensive_rating"] == 115.2
