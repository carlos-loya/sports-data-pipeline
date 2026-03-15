# CLAUDE.md

## Project Overview

Sports data pipeline using medallion architecture (Bronze → Silver → Gold) for NFL, NBA, and Soccer data. Built with Python, Parquet, DuckDB, and Airflow.

## Build & Test Commands

- `make install-dev` — Install with dev dependencies (uses uv)
- `make test` — Run tests (`uv run pytest`)
- `make test-cov` — Run tests with coverage
- `make lint` — Lint check (`uv run ruff check .`)
- `make format` — Auto-format (`uv run ruff format .`)
- `make check` — Lint + test combined
- `make init-db` — Initialize DuckDB schema

## Code Style

- Formatter/linter: ruff (line length 100, target Python 3.11)
- Use `datetime.now(UTC)` — never `datetime.utcnow()` (deprecated in 3.12+)
- Type hints required on all function signatures
- Pydantic models for all data schemas

## Documentation Requirements

**When adding or modifying tables, columns, or data transformations, you must update `docs/data-dictionary.md` to reflect the changes.** This includes:

- New extractors or data sources
- New or renamed columns in Bronze, Silver, or Gold models
- New or changed derived fields in transformers
- New DuckDB tables or views in the Gold layer
- Changes to nullability, types, or default values

The data dictionary is the primary reference for users of this pipeline. Keeping it accurate is a hard requirement, not optional.
