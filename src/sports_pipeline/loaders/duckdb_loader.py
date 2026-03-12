"""DuckDB loader for gold-layer data."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from sports_pipeline.storage.paths import gold_db_path
from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


class DuckDBLoader:
    """Load DataFrames into DuckDB gold layer tables."""

    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = db_path or gold_db_path()
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    def get_connection(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        """Get a DuckDB connection."""
        return duckdb.connect(str(self._db_path), read_only=read_only)

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = "gold",
        mode: str = "append",
    ) -> int:
        """Load a DataFrame into a DuckDB table.

        Args:
            df: DataFrame to load
            table_name: Target table name
            schema: Schema name (default "gold")
            mode: "append" or "replace"

        Returns:
            Number of rows loaded.
        """
        if df.empty:
            log.warning("empty_dataframe", table=f"{schema}.{table_name}")
            return 0

        full_table = f"{schema}.{table_name}"
        conn = self.get_connection()

        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

            if mode == "replace":
                conn.execute(f"DROP TABLE IF EXISTS {full_table}")
                conn.execute(f"CREATE TABLE {full_table} AS SELECT * FROM df")
            else:
                # Try to append; create if doesn't exist
                try:
                    conn.execute(f"INSERT INTO {full_table} SELECT * FROM df")
                except duckdb.CatalogException:
                    conn.execute(f"CREATE TABLE {full_table} AS SELECT * FROM df")

            count = conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
            log.info("loaded_to_duckdb", table=full_table, rows_loaded=len(df), total_rows=count)
            return len(df)
        finally:
            conn.close()

    def execute(self, sql: str) -> None:
        """Execute a SQL statement."""
        conn = self.get_connection()
        try:
            conn.execute(sql)
        finally:
            conn.close()

    def query(self, sql: str) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        conn = self.get_connection(read_only=True)
        try:
            return conn.execute(sql).fetchdf()
        finally:
            conn.close()
