"""Deduplication utilities for silver layer."""

from __future__ import annotations

import pandas as pd

from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


def deduplicate(df: pd.DataFrame, subset: list[str], keep: str = "last") -> pd.DataFrame:
    """Remove duplicate rows based on key columns.

    Args:
        df: Input DataFrame
        subset: Columns to use for identifying duplicates
        keep: Which duplicate to keep ("first" or "last")

    Returns:
        Deduplicated DataFrame.
    """
    before = len(df)
    df = df.drop_duplicates(subset=subset, keep=keep)
    after = len(df)
    if before != after:
        log.info("deduplication", removed=before - after, remaining=after, keys=subset)
    return df
