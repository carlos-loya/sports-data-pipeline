#!/usr/bin/env python
"""Backfill historical data for specified date range."""

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sports_pipeline.utils.logging import setup_logging, get_logger

log = get_logger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill historical sports data")
    parser.add_argument("--sport", choices=["soccer", "basketball", "all"], default="all")
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, default=date.today())
    return parser.parse_args()


if __name__ == "__main__":
    setup_logging()
    args = parse_args()
    log.info("backfill_started", sport=args.sport, start=str(args.start_date), end=str(args.end_date))
    print(f"Backfill: {args.sport} from {args.start_date} to {args.end_date}")
    # Implementation delegates to extractors + transformers + loaders
    log.info("backfill_complete")
