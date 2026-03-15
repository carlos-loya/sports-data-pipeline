#!/usr/bin/env python
"""Backfill historical data for specified sports and seasons."""

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sports_pipeline.config import PROJECT_ROOT, get_settings
from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
from sports_pipeline.loaders.gold_builder import GoldBuilder
from sports_pipeline.loaders.views import refresh_views
from sports_pipeline.storage.parquet_store import read_parquet_dir, write_parquet
from sports_pipeline.storage.paths import bronze_path
from sports_pipeline.utils.logging import get_logger, setup_logging

log = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill historical sports data")
    parser.add_argument(
        "--sport",
        choices=["soccer", "basketball", "football", "all"],
        default="all",
        help="Sport to backfill (default: all)",
    )
    parser.add_argument(
        "--season",
        type=str,
        default=None,
        help="Specific season to backfill (e.g. '2024-2025', '2024-25', '2024'). "
        "If omitted, all configured seasons are backfilled.",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip extraction and only run transform/load on existing bronze data",
    )
    parser.add_argument(
        "--skip-gold",
        action="store_true",
        help="Skip gold feature building",
    )
    return parser.parse_args()


def extract_soccer(season_filter: str | None = None) -> int:
    """Extract soccer data from FBref for configured leagues and seasons."""
    from sports_pipeline.extractors.fbref.match_extractor import FbrefMatchExtractor
    from sports_pipeline.extractors.fbref.team_extractor import FbrefTeamExtractor

    settings = get_settings()
    match_extractor = FbrefMatchExtractor()
    team_extractor = FbrefTeamExtractor()
    total = 0
    today = date.today()

    for league in settings.leagues.soccer:
        seasons = [season_filter] if season_filter else league.seasons
        for season in seasons:
            log.info("extracting_soccer", league=league.name, season=season)

            try:
                match_df = match_extractor.extract(
                    league_id=league.fbref_id,
                    season=season,
                    league_name=league.name,
                )
                if not match_df.empty:
                    path = bronze_path(
                        "soccer",
                        f"matches_{league.name.lower().replace(' ', '_')}",
                        season,
                        today,
                    )
                    write_parquet(match_df, path)
                    total += len(match_df)
                    log.info("soccer_matches_extracted", league=league.name, rows=len(match_df))
            except Exception as e:
                log.error("soccer_match_extraction_failed", league=league.name, error=str(e))

            try:
                team_df = team_extractor.extract(
                    league_id=league.fbref_id,
                    season=season,
                    league_name=league.name,
                )
                if not team_df.empty:
                    path = bronze_path(
                        "soccer",
                        f"teams_{league.name.lower().replace(' ', '_')}",
                        season,
                        today,
                    )
                    write_parquet(team_df, path)
                    total += len(team_df)
                    log.info("soccer_teams_extracted", league=league.name, rows=len(team_df))
            except Exception as e:
                log.error("soccer_team_extraction_failed", league=league.name, error=str(e))

    return total


def extract_basketball(season_filter: str | None = None) -> int:
    """Extract NBA data for configured seasons."""
    from sports_pipeline.extractors.nba.game_extractor import NbaGameExtractor
    from sports_pipeline.extractors.nba.team_extractor import NbaTeamExtractor

    settings = get_settings()
    game_extractor = NbaGameExtractor()
    team_extractor = NbaTeamExtractor()
    total = 0
    today = date.today()

    for league in settings.leagues.basketball:
        seasons = [season_filter] if season_filter else league.seasons
        for season in seasons:
            log.info("extracting_basketball", season=season)

            try:
                game_df = game_extractor.extract(season=season)
                if not game_df.empty:
                    path = bronze_path("basketball", "games", season, today)
                    write_parquet(game_df, path)
                    total += len(game_df)
                    log.info("nba_games_extracted", rows=len(game_df))
            except Exception as e:
                log.error("nba_game_extraction_failed", error=str(e))

            try:
                team_df = team_extractor.extract(season=season)
                if not team_df.empty:
                    path = bronze_path("basketball", "team_stats", season, today)
                    write_parquet(team_df, path)
                    total += len(team_df)
                    log.info("nba_teams_extracted", rows=len(team_df))
            except Exception as e:
                log.error("nba_team_extraction_failed", error=str(e))

    return total


def extract_football(season_filter: str | None = None) -> int:
    """Extract NFL data for configured seasons."""
    from sports_pipeline.extractors.nfl.game_extractor import NflGameExtractor
    from sports_pipeline.extractors.nfl.player_extractor import NflPlayerExtractor
    from sports_pipeline.extractors.nfl.team_extractor import NflTeamExtractor

    settings = get_settings()
    game_extractor = NflGameExtractor()
    player_extractor = NflPlayerExtractor()
    team_extractor = NflTeamExtractor()
    total = 0
    today = date.today()

    for league in settings.leagues.football:
        seasons = [season_filter] if season_filter else league.seasons
        for season in seasons:
            int_season = int(season)
            log.info("extracting_football", season=season)

            try:
                game_df = game_extractor.extract(season=int_season)
                if not game_df.empty:
                    path = bronze_path("football", "games", season, today)
                    write_parquet(game_df, path)
                    total += len(game_df)
                    log.info("nfl_games_extracted", rows=len(game_df))
            except Exception as e:
                log.error("nfl_game_extraction_failed", error=str(e))

            try:
                player_df = player_extractor.extract(season=int_season)
                if not player_df.empty:
                    path = bronze_path("football", "player_stats", season, today)
                    write_parquet(player_df, path)
                    total += len(player_df)
                    log.info("nfl_players_extracted", rows=len(player_df))
            except Exception as e:
                log.error("nfl_player_extraction_failed", error=str(e))

            try:
                team_df = team_extractor.extract(season=int_season)
                if not team_df.empty:
                    path = bronze_path("football", "team_stats", season, today)
                    write_parquet(team_df, path)
                    total += len(team_df)
                    log.info("nfl_teams_extracted", rows=len(team_df))
            except Exception as e:
                log.error("nfl_team_extraction_failed", error=str(e))

    return total


def transform_and_load() -> None:
    """Read bronze data, transform, and load into DuckDB gold tables."""
    from sports_pipeline.transformers.nba.game_transformer import NbaGameTransformer
    from sports_pipeline.transformers.nfl.game_transformer import NflGameTransformer
    from sports_pipeline.transformers.nfl.player_transformer import NflPlayerTransformer
    from sports_pipeline.transformers.soccer.match_transformer import SoccerMatchTransformer

    settings = get_settings()
    loader = DuckDBLoader()

    # Soccer
    try:
        soccer_dir = PROJECT_ROOT / settings.storage.bronze_path / "soccer"
        if soccer_dir.exists():
            soccer_df = read_parquet_dir(soccer_dir)
            if not soccer_df.empty:
                silver = SoccerMatchTransformer().transform(soccer_df)
                if not silver.empty:
                    loader.load_dataframe(silver, "soccer_matches", mode="replace")
                    log.info("soccer_loaded", rows=len(silver))
    except Exception as e:
        log.error("soccer_transform_load_failed", error=str(e))

    # Basketball
    try:
        nba_dir = PROJECT_ROOT / settings.storage.bronze_path / "basketball"
        if nba_dir.exists():
            nba_df = read_parquet_dir(nba_dir)
            if not nba_df.empty:
                silver = NbaGameTransformer().transform(nba_df)
                if not silver.empty:
                    loader.load_dataframe(silver, "nba_games", mode="replace")
                    log.info("nba_loaded", rows=len(silver))
    except Exception as e:
        log.error("nba_transform_load_failed", error=str(e))

    # Football
    try:
        football_dir = PROJECT_ROOT / settings.storage.bronze_path / "football"
        if football_dir.exists():
            football_df = read_parquet_dir(football_dir)
            if not football_df.empty:
                game_silver = NflGameTransformer().transform(football_df)
                if not game_silver.empty:
                    loader.load_dataframe(game_silver, "nfl_games", mode="replace")
                    log.info("nfl_games_loaded", rows=len(game_silver))

                player_silver = NflPlayerTransformer().transform(football_df)
                if not player_silver.empty:
                    loader.load_dataframe(player_silver, "nfl_players", mode="replace")
                    log.info("nfl_players_loaded", rows=len(player_silver))
    except Exception as e:
        log.error("nfl_transform_load_failed", error=str(e))


def build_gold() -> None:
    """Build gold feature tables and refresh views."""
    builder = GoldBuilder()
    builder.build_soccer_team_form()
    builder.build_soccer_h2h()
    builder.build_nba_team_form()
    refresh_views()
    log.info("gold_features_built")


if __name__ == "__main__":
    setup_logging()
    args = parse_args()

    log.info("backfill_started", sport=args.sport, season=args.season)

    # Extract
    if not args.skip_extract:
        extract_fns = {
            "soccer": extract_soccer,
            "basketball": extract_basketball,
            "football": extract_football,
        }

        if args.sport == "all":
            for sport_name, fn in extract_fns.items():
                log.info("extracting_sport", sport=sport_name)
                count = fn(season_filter=args.season)
                log.info("extraction_complete", sport=sport_name, total_rows=count)
        else:
            count = extract_fns[args.sport](season_filter=args.season)
            log.info("extraction_complete", sport=args.sport, total_rows=count)
    else:
        log.info("skipping_extraction")

    # Transform and load
    log.info("transform_and_load_started")
    transform_and_load()
    log.info("transform_and_load_complete")

    # Build gold features
    if not args.skip_gold:
        log.info("building_gold_features")
        build_gold()

    log.info("backfill_complete")
