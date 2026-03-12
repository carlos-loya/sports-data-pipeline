#!/usr/bin/env python
"""Initialize the DuckDB database with schema and views."""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sports_pipeline.loaders.views import init_schema
from sports_pipeline.utils.logging import setup_logging

if __name__ == "__main__":
    setup_logging()
    init_schema()
    print("DuckDB schema initialized successfully.")
