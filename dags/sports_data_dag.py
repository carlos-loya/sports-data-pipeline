"""Airflow DAG for daily sports data ingestion (FBref + nba_api + nflreadpy)."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "sports-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def extract_nba_games(**context):
    from datetime import date

    from sports_pipeline.extractors.nba.game_extractor import NbaGameExtractor
    from sports_pipeline.storage.parquet_store import write_parquet
    from sports_pipeline.storage.paths import bronze_path

    season = context["params"].get("season", "2024-25")
    extractor = NbaGameExtractor()
    df = extractor.extract(season=season)
    if not df.empty:
        path = bronze_path("basketball", "games", season, date.today())
        write_parquet(df, path)
    return len(df)


def extract_nba_team_stats(**context):
    from datetime import date

    from sports_pipeline.extractors.nba.team_extractor import NbaTeamExtractor
    from sports_pipeline.storage.parquet_store import write_parquet
    from sports_pipeline.storage.paths import bronze_path

    season = context["params"].get("season", "2024-25")
    extractor = NbaTeamExtractor()
    df = extractor.extract(season=season)
    if not df.empty:
        path = bronze_path("basketball", "team_stats", season, date.today())
        write_parquet(df, path)
    return len(df)


def extract_fbref_matches(**context):
    from datetime import date

    from sports_pipeline.config import get_settings
    from sports_pipeline.extractors.fbref.match_extractor import FbrefMatchExtractor
    from sports_pipeline.storage.parquet_store import write_parquet
    from sports_pipeline.storage.paths import bronze_path

    settings = get_settings()
    extractor = FbrefMatchExtractor()

    total = 0
    for league in settings.leagues.soccer:
        df = extractor.extract(
            league_id=league.fbref_id,
            season=league.seasons[0],
            league_name=league.name,
        )
        if not df.empty:
            path = bronze_path(
                "soccer",
                f"matches_{league.name.lower().replace(' ', '_')}",
                league.seasons[0],
                date.today(),
            )
            write_parquet(df, path)
            total += len(df)
    return total


def extract_fbref_teams(**context):
    from datetime import date

    from sports_pipeline.config import get_settings
    from sports_pipeline.extractors.fbref.team_extractor import FbrefTeamExtractor
    from sports_pipeline.storage.parquet_store import write_parquet
    from sports_pipeline.storage.paths import bronze_path

    settings = get_settings()
    extractor = FbrefTeamExtractor()

    total = 0
    for league in settings.leagues.soccer:
        df = extractor.extract(
            league_id=league.fbref_id,
            season=league.seasons[0],
            league_name=league.name,
        )
        if not df.empty:
            path = bronze_path(
                "soccer",
                f"teams_{league.name.lower().replace(' ', '_')}",
                league.seasons[0],
                date.today(),
            )
            write_parquet(df, path)
            total += len(df)
    return total


def extract_nfl_games(**context):
    from datetime import date

    from sports_pipeline.extractors.nfl.game_extractor import NflGameExtractor
    from sports_pipeline.storage.parquet_store import write_parquet
    from sports_pipeline.storage.paths import bronze_path

    season = context["params"].get("nfl_season", 2024)
    extractor = NflGameExtractor()
    df = extractor.extract(season=season)
    if not df.empty:
        path = bronze_path("football", "games", str(season), date.today())
        write_parquet(df, path)
    return len(df)


def extract_nfl_player_stats(**context):
    from datetime import date

    from sports_pipeline.extractors.nfl.player_extractor import NflPlayerExtractor
    from sports_pipeline.storage.parquet_store import write_parquet
    from sports_pipeline.storage.paths import bronze_path

    season = context["params"].get("nfl_season", 2024)
    extractor = NflPlayerExtractor()
    df = extractor.extract(season=season)
    if not df.empty:
        path = bronze_path("football", "player_stats", str(season), date.today())
        write_parquet(df, path)
    return len(df)


def extract_nfl_team_stats(**context):
    from datetime import date

    from sports_pipeline.extractors.nfl.team_extractor import NflTeamExtractor
    from sports_pipeline.storage.parquet_store import write_parquet
    from sports_pipeline.storage.paths import bronze_path

    season = context["params"].get("nfl_season", 2024)
    extractor = NflTeamExtractor()
    df = extractor.extract(season=season)
    if not df.empty:
        path = bronze_path("football", "team_stats", str(season), date.today())
        write_parquet(df, path)
    return len(df)


def transform_and_load(**context):
    from sports_pipeline.config import PROJECT_ROOT, get_settings
    from sports_pipeline.loaders.duckdb_loader import DuckDBLoader
    from sports_pipeline.storage.parquet_store import read_parquet_dir
    from sports_pipeline.transformers.nba.game_transformer import NbaGameTransformer
    from sports_pipeline.transformers.soccer.match_transformer import SoccerMatchTransformer

    loader = DuckDBLoader()

    # Transform and load NBA
    try:
        nba_bronze_dir = PROJECT_ROOT / get_settings().storage.bronze_path / "basketball"
        if nba_bronze_dir.exists():
            nba_df = read_parquet_dir(nba_bronze_dir)
            nba_transformer = NbaGameTransformer()
            nba_silver = nba_transformer.transform(nba_df)
            if not nba_silver.empty:
                loader.load_dataframe(nba_silver, "nba_games", mode="replace")
    except Exception as e:
        print(f"NBA transform/load error: {e}")

    # Transform and load Soccer
    try:
        soccer_bronze_dir = PROJECT_ROOT / get_settings().storage.bronze_path / "soccer"
        if soccer_bronze_dir.exists():
            soccer_df = read_parquet_dir(soccer_bronze_dir)
            soccer_transformer = SoccerMatchTransformer()
            soccer_silver = soccer_transformer.transform(soccer_df)
            if not soccer_silver.empty:
                loader.load_dataframe(soccer_silver, "soccer_matches", mode="replace")
    except Exception as e:
        print(f"Soccer transform/load error: {e}")

    # Transform and load NFL
    try:
        from sports_pipeline.transformers.nfl.game_transformer import NflGameTransformer
        from sports_pipeline.transformers.nfl.player_transformer import NflPlayerTransformer

        football_bronze_dir = PROJECT_ROOT / get_settings().storage.bronze_path / "football"
        if football_bronze_dir.exists():
            football_df = read_parquet_dir(football_bronze_dir)
            if not football_df.empty:
                nfl_game_transformer = NflGameTransformer()
                nfl_game_silver = nfl_game_transformer.transform(football_df)
                if not nfl_game_silver.empty:
                    loader.load_dataframe(nfl_game_silver, "nfl_games", mode="replace")

                nfl_player_transformer = NflPlayerTransformer()
                nfl_player_silver = nfl_player_transformer.transform(football_df)
                if not nfl_player_silver.empty:
                    loader.load_dataframe(nfl_player_silver, "nfl_players", mode="replace")
    except Exception as e:
        print(f"NFL transform/load error: {e}")


def build_gold_features(**context):
    from sports_pipeline.loaders.gold_builder import GoldBuilder
    from sports_pipeline.loaders.views import refresh_views

    builder = GoldBuilder()
    builder.build_soccer_team_form()
    builder.build_soccer_h2h()
    builder.build_nba_team_form()
    refresh_views()


with DAG(
    dag_id="sports_data_pipeline",
    default_args=default_args,
    description="Daily sports data ingestion and processing",
    schedule="0 6 * * *",  # 06:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sports", "ingestion"],
    params={"season": "2024-25", "nfl_season": 2024},
) as dag:
    with TaskGroup("nba_extraction") as nba_group:
        nba_games = PythonOperator(task_id="extract_nba_games", python_callable=extract_nba_games)
        nba_teams = PythonOperator(
            task_id="extract_nba_team_stats", python_callable=extract_nba_team_stats
        )
        [nba_games, nba_teams]

    with TaskGroup("soccer_extraction") as soccer_group:
        fbref_matches = PythonOperator(
            task_id="extract_fbref_matches", python_callable=extract_fbref_matches
        )
        fbref_teams = PythonOperator(
            task_id="extract_fbref_teams", python_callable=extract_fbref_teams
        )
        fbref_matches >> fbref_teams  # Sequential due to rate limits

    with TaskGroup("nfl_extraction") as nfl_group:
        nfl_games = PythonOperator(task_id="extract_nfl_games", python_callable=extract_nfl_games)
        nfl_players = PythonOperator(
            task_id="extract_nfl_player_stats", python_callable=extract_nfl_player_stats
        )
        nfl_teams = PythonOperator(
            task_id="extract_nfl_team_stats", python_callable=extract_nfl_team_stats
        )
        [nfl_games, nfl_players, nfl_teams]

    transform = PythonOperator(task_id="transform_and_load", python_callable=transform_and_load)
    gold = PythonOperator(task_id="build_gold_features", python_callable=build_gold_features)

    [nba_group, soccer_group, nfl_group] >> transform >> gold
