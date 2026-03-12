"""Parquet read/write utilities."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


def write_parquet(df: pd.DataFrame, path: Path) -> Path:
    """Write a DataFrame to a Parquet file, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)
    log.info("wrote_parquet", path=str(path), rows=len(df))
    return path


def read_parquet(path: Path) -> pd.DataFrame:
    """Read a Parquet file into a DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")
    return pq.read_table(path).to_pandas()


def read_parquet_dir(directory: Path, pattern: str = "*.parquet") -> pd.DataFrame:
    """Read all Parquet files matching pattern in a directory tree."""
    files = sorted(directory.rglob(pattern))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {directory}")
    dfs = [pq.read_table(f).to_pandas() for f in files]
    return pd.concat(dfs, ignore_index=True)
