"""Airflow DAG for weekly maintenance tasks."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "sports-pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def vacuum_duckdb(**context):
    from sports_pipeline.loaders.duckdb_loader import DuckDBLoader

    loader = DuckDBLoader()
    loader.execute("VACUUM")


with DAG(
    dag_id="maintenance_pipeline",
    default_args=default_args,
    description="Weekly maintenance: vacuum DuckDB",
    schedule="0 2 * * 0",  # Sunday 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["maintenance"],
) as dag:
    vacuum = PythonOperator(task_id="vacuum_duckdb", python_callable=vacuum_duckdb)
