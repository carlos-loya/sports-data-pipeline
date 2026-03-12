"""Data quality reporting."""

from __future__ import annotations

import pandas as pd

from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


def log_quality_summary(df: pd.DataFrame, name: str) -> dict:
    """Log a summary of data quality metrics for a DataFrame."""
    summary = {
        "name": name,
        "rows": len(df),
        "columns": len(df.columns),
        "null_pct": {col: round(df[col].isna().mean(), 3) for col in df.columns},
        "duplicates": int(df.duplicated().sum()),
    }
    log.info("quality_summary", **summary)
    return summary
