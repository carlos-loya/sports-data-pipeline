"""Path utilities for the medallion architecture storage layout."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from sports_pipeline.config import PROJECT_ROOT, get_settings


def _storage_root() -> Path:
    return PROJECT_ROOT / get_settings().storage.bronze_path


def bronze_path(sport: str, data_type: str, season: str, dt: date) -> Path:
    """Return bronze parquet path: data/bronze/{sport}/season={s}/date={d}/{data_type}.parquet"""
    settings = get_settings()
    base = PROJECT_ROOT / settings.storage.bronze_path
    return base / sport / f"season={season}" / f"date={dt.isoformat()}" / f"{data_type}.parquet"


def bronze_kalshi_path(dt: date, data_type: str = "markets") -> Path:
    """Return bronze Kalshi parquet path: data/bronze/kalshi/date={d}/{data_type}.parquet"""
    settings = get_settings()
    base = PROJECT_ROOT / settings.storage.bronze_path
    return base / "kalshi" / f"date={dt.isoformat()}" / f"{data_type}.parquet"


def silver_path(sport: str, data_type: str) -> Path:
    """Return silver parquet path: data/silver/{sport}/{data_type}.parquet"""
    settings = get_settings()
    base = PROJECT_ROOT / settings.storage.silver_path
    return base / sport / f"{data_type}.parquet"


def silver_kalshi_path(data_type: str = "markets") -> Path:
    """Return silver Kalshi parquet path."""
    settings = get_settings()
    base = PROJECT_ROOT / settings.storage.silver_path
    return base / "kalshi" / f"{data_type}.parquet"


def gold_db_path() -> Path:
    """Return path to the DuckDB database."""
    settings = get_settings()
    return PROJECT_ROOT / settings.storage.duckdb_path
