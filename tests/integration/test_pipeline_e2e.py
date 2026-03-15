"""End-to-end integration tests for the sports data pipeline.

These tests exercise the full pipeline chain:
  Extract (mocked API) → Transform → Parquet write/read → DuckDB load → View query

External APIs are mocked, but all internal boundaries between pipeline stages
are real — this catches column mismatches, type conflicts, and schema drift
that unit tests with mocked internals would miss.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from sports_pipeline.extractors.nba.game_extractor import NbaGameExtractor
from sports_pipeline.extractors.nba.player_extractor import NbaPlayerExtractor
from sports_pipeline.extractors.nfl.game_extractor import NflGameExtractor
from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
from sports_pipeline.loaders.gold_builder import GoldBuilder
from sports_pipeline.loaders.views import init_schema, refresh_views
from sports_pipeline.storage.parquet_store import read_parquet, write_parquet
from sports_pipeline.transformers.nba.game_transformer import NbaGameTransformer
from sports_pipeline.transformers.nfl.game_transformer import NflGameTransformer
from sports_pipeline.transformers.soccer.match_transformer import SoccerMatchTransformer

# ---------------------------------------------------------------------------
# NFL: Extract → Transform → Parquet → Gold → Standings View
# ---------------------------------------------------------------------------


class TestNflPipeline:
    """NFL end-to-end: mock client → extractor → transformer → parquet → DuckDB → view."""

    def test_extract_transform_load_standings(self, sample_nfl_schedule, tmp_path):
        # --- Extract ---
        client = MagicMock()
        client.get_schedules.return_value = sample_nfl_schedule
        extractor = NflGameExtractor(client=client)
        bronze = extractor.extract(season=2024)

        assert not bronze.empty
        assert "game_id" in bronze.columns
        assert "home_team" in bronze.columns

        # --- Transform ---
        transformer = NflGameTransformer()
        silver = transformer.transform(bronze)

        assert not silver.empty
        assert "home_win" in silver.columns
        assert "total_points" in silver.columns
        # Team names should be normalized from abbreviations
        assert silver.iloc[0]["home_team"] != "BAL"  # should be full name

        # --- Parquet round-trip ---
        parquet_path = tmp_path / "nfl" / "season=2024" / "date=2024-09-05" / "games.parquet"
        write_parquet(silver, parquet_path)
        silver_read = read_parquet(parquet_path)

        assert len(silver_read) == len(silver)
        assert set(silver_read.columns) == set(silver.columns)

        # --- Gold load + view ---
        db = DuckDBLoader(db_path=tmp_path / "test.duckdb")
        init_schema(db)
        db.load_dataframe(silver, "nfl_games", mode="replace")
        refresh_views(db)

        standings = db.query("SELECT * FROM gold.v_nfl_standings ORDER BY rank")
        assert not standings.empty
        assert "wins" in standings.columns
        assert "win_pct" in standings.columns
        assert "point_differential" in standings.columns

        # KC won both games (27-20 @BAL, 26-25 @CIN)
        kc = standings[standings["team"] == "Kansas City Chiefs"].iloc[0]
        assert kc["wins"] == 2
        assert kc["losses"] == 0

    def test_parquet_roundtrip_preserves_types(self, sample_nfl_schedule, tmp_path):
        """Verify Parquet write→read doesn't corrupt nullable int types."""
        client = MagicMock()
        client.get_schedules.return_value = sample_nfl_schedule
        bronze = NflGameExtractor(client=client).extract(season=2024)
        silver = NflGameTransformer().transform(bronze)

        path = tmp_path / "nfl" / "games.parquet"
        write_parquet(silver, path)
        read_back = read_parquet(path)

        # Scores should survive round-trip as numeric
        assert pd.api.types.is_numeric_dtype(read_back["home_score"])
        assert pd.api.types.is_numeric_dtype(read_back["away_score"])
        # Boolean should survive
        assert read_back["overtime"].dtype in ("bool", "boolean")

    def test_hive_partitioned_read(self, sample_nfl_schedule, tmp_path):
        """Verify reads work from Hive-partitioned directories without ArrowTypeError."""
        client = MagicMock()
        client.get_schedules.return_value = sample_nfl_schedule
        bronze = NflGameExtractor(client=client).extract(season=2024)
        silver = NflGameTransformer().transform(bronze)

        # Write to a Hive-partitioned path (season=2024/date=...)
        path = tmp_path / "bronze" / "nfl" / "season=2024" / "date=2024-09-05" / "games.parquet"
        write_parquet(silver, path)

        # This used to raise ArrowTypeError before the fix
        read_back = read_parquet(path)
        assert len(read_back) == len(silver)


# ---------------------------------------------------------------------------
# NBA: Extract → Transform → Parquet → Gold → Standings View
# ---------------------------------------------------------------------------


class TestNbaPipeline:
    """NBA end-to-end: mock client → extractor → transformer → parquet → DuckDB → view."""

    def test_extract_transform_load_standings(self, sample_nba_game_log, tmp_path):
        # --- Extract ---
        client = MagicMock()
        client.get_league_game_log.return_value = sample_nba_game_log
        extractor = NbaGameExtractor(client=client)
        bronze = extractor.extract(season="2024-25")

        assert not bronze.empty
        assert "home_team_name" in bronze.columns
        assert "away_team_name" in bronze.columns

        # --- Transform ---
        transformer = NbaGameTransformer()
        silver = transformer.transform(bronze)

        assert not silver.empty
        assert "home_win" in silver.columns
        assert "total_points" in silver.columns

        # --- Parquet round-trip ---
        path = tmp_path / "nba" / "games.parquet"
        write_parquet(silver, path)
        silver_read = read_parquet(path)
        assert len(silver_read) == len(silver)

        # --- Gold load + view ---
        db = DuckDBLoader(db_path=tmp_path / "test.duckdb")
        init_schema(db)
        db.load_dataframe(silver, "nba_games", mode="replace")
        refresh_views(db)

        standings = db.query("SELECT * FROM gold.v_nba_standings ORDER BY rank")
        assert not standings.empty
        assert "win_pct" in standings.columns

        # Lakers won the game (110-105)
        lakers = standings[standings["team"].str.contains("Lakers")].iloc[0]
        assert lakers["wins"] == 1
        assert lakers["losses"] == 0

    def test_player_extract_transform_roundtrip(self, sample_nba_player_log, tmp_path):
        """Verify player extractor output survives parquet round-trip."""
        client = MagicMock()
        client.get_player_game_log.return_value = sample_nba_player_log
        extractor = NbaPlayerExtractor(client=client)

        bronze = extractor.extract(player_id=2544, season="2024-25", player_name="LeBron James")

        assert not bronze.empty
        assert bronze.iloc[0]["player_name"] == "LeBron James"
        assert bronze.iloc[0]["team_name"] == "LAL"

        # Parquet round-trip
        path = tmp_path / "nba" / "players.parquet"
        write_parquet(bronze, path)
        read_back = read_parquet(path)

        assert read_back.iloc[0]["player_name"] == "LeBron James"
        assert read_back.iloc[0]["team_name"] == "LAL"
        assert pd.api.types.is_numeric_dtype(read_back["points"])


# ---------------------------------------------------------------------------
# Soccer: Extract → Transform → Parquet → Gold → Standings + Builder
# ---------------------------------------------------------------------------


class TestSoccerPipeline:
    """Soccer end-to-end: bronze fixture → transformer → parquet → DuckDB → views + builder."""

    def test_transform_load_standings(self, sample_bronze_soccer_matches, tmp_path):
        # --- Transform ---
        transformer = SoccerMatchTransformer()
        silver = transformer.transform(sample_bronze_soccer_matches)

        assert not silver.empty
        assert "match_id" in silver.columns
        assert "result" in silver.columns
        # Arsenal 2-0 Wolves → result should be "H"
        arsenal_match = silver[silver["home_team"].str.contains("Arsenal")]
        assert not arsenal_match.empty
        assert arsenal_match.iloc[0]["result"] == "H"

        # --- Parquet round-trip ---
        path = tmp_path / "soccer" / "matches.parquet"
        write_parquet(silver, path)
        silver_read = read_parquet(path)
        assert len(silver_read) == len(silver)

        # --- Gold load + views ---
        db = DuckDBLoader(db_path=tmp_path / "test.duckdb")
        init_schema(db)
        db.load_dataframe(silver, "soccer_matches", mode="replace")
        refresh_views(db)

        standings = db.query("SELECT * FROM gold.v_soccer_league_standings ORDER BY rank")
        assert not standings.empty
        assert "points" in standings.columns

        # Arsenal won (3 pts), Man City drew (1 pt)
        s = standings.set_index("team")
        arsenal = s.loc[s.index.str.contains("Arsenal")]
        assert arsenal.iloc[0]["points"] == 3

    def test_gold_builder_team_form(self, sample_bronze_soccer_matches, tmp_path):
        """Verify GoldBuilder can build team form from loaded data."""
        silver = SoccerMatchTransformer().transform(sample_bronze_soccer_matches)

        db = DuckDBLoader(db_path=tmp_path / "test.duckdb")
        init_schema(db)
        db.load_dataframe(silver, "soccer_matches", mode="replace")

        builder = GoldBuilder(loader=db)
        form = builder.build_soccer_team_form(n_matches=5)

        assert not form.empty
        assert "form_string" in form.columns
        assert "points" in form.columns

    def test_gold_builder_h2h(self, sample_bronze_soccer_matches, tmp_path):
        """Verify GoldBuilder can build head-to-head from loaded data."""
        silver = SoccerMatchTransformer().transform(sample_bronze_soccer_matches)

        db = DuckDBLoader(db_path=tmp_path / "test.duckdb")
        init_schema(db)
        db.load_dataframe(silver, "soccer_matches", mode="replace")

        builder = GoldBuilder(loader=db)
        h2h = builder.build_soccer_h2h()

        assert not h2h.empty
        assert "team_a_wins" in h2h.columns


# ---------------------------------------------------------------------------
# Cross-cutting: Parquet storage edge cases
# ---------------------------------------------------------------------------


class TestParquetStorage:
    """Integration tests for Parquet write/read across partition layouts."""

    def test_read_parquet_dir(self, sample_nfl_schedule, tmp_path):
        """Verify read_parquet_dir concatenates multiple files correctly."""
        from sports_pipeline.storage.parquet_store import read_parquet_dir

        client = MagicMock()
        client.get_schedules.return_value = sample_nfl_schedule
        bronze = NflGameExtractor(client=client).extract(season=2024)
        silver = NflGameTransformer().transform(bronze)

        # Split into two files in different date partitions
        week1 = silver[silver["week"] == 1]
        week2 = silver[silver["week"] == 2]

        write_parquet(week1, tmp_path / "nfl" / "season=2024" / "date=2024-09-05" / "games.parquet")
        write_parquet(week2, tmp_path / "nfl" / "season=2024" / "date=2024-09-15" / "games.parquet")

        combined = read_parquet_dir(tmp_path / "nfl")
        assert len(combined) == len(silver)

    def test_write_creates_parent_dirs(self, tmp_path):
        """Verify write_parquet creates nested directories."""
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        deep_path = tmp_path / "a" / "b" / "c" / "data.parquet"
        result = write_parquet(df, deep_path)
        assert result.exists()
        assert len(read_parquet(result)) == 2
