"""Tests for data transformers."""

from __future__ import annotations

import pandas as pd

from sports_pipeline.transformers.nba.game_transformer import NbaGameTransformer
from sports_pipeline.transformers.soccer.match_transformer import SoccerMatchTransformer


class TestNbaGameTransformer:
    def test_transform_basic(self, sample_bronze_nba_games):
        transformer = NbaGameTransformer()
        result = transformer.transform(sample_bronze_nba_games)

        assert not result.empty
        assert "home_win" in result.columns
        assert "total_points" in result.columns
        assert result.iloc[0]["home_win"]
        assert result.iloc[0]["total_points"] == 215

    def test_transform_empty(self):
        transformer = NbaGameTransformer()
        result = transformer.transform(pd.DataFrame())
        assert result.empty


class TestSoccerMatchTransformer:
    def test_transform_basic(self, sample_bronze_soccer_matches):
        transformer = SoccerMatchTransformer()
        result = transformer.transform(sample_bronze_soccer_matches)

        assert len(result) == 2
        assert "match_id" in result.columns
        assert "result" in result.columns

        arsenal_match = result[result["home_team"] == "Arsenal"].iloc[0]
        assert arsenal_match["result"] == "H"
        assert arsenal_match["home_goals"] == 2

        city_match = result[result["home_team"] == "Manchester City"].iloc[0]
        assert city_match["result"] == "D"

    def test_transform_empty(self):
        transformer = SoccerMatchTransformer()
        result = transformer.transform(pd.DataFrame())
        assert result.empty
