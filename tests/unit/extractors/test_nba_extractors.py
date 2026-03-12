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
        client.get_player_game_log.return_value = sample_nba_player_log
        extractor = NbaPlayerExtractor(client=client)

        result = extractor.extract(player_id=2544, season="2024-25")

        assert not result.empty
        assert result.iloc[0]["player_name"] == "LeBron James"
        assert result.iloc[0]["points"] == 28
        assert result.iloc[0]["rebounds"] == 8
        assert result.iloc[0]["assists"] == 10


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
