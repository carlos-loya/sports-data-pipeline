# Sports Data Pipeline

Sports data collection and ETL pipeline with medallion architecture (Bronze → Silver → Gold).

## Data Sources

- **FBref** — Soccer match results, team stats, player stats (Premier League, La Liga, Serie A, Bundesliga, Ligue 1)
- **nba_api** — NBA game results, team stats, player stats
- **nflreadpy** — NFL game schedules/scores and player stats (passing, rushing, receiving)

## Architecture

```
FBref / nba_api / nflreadpy  →  Bronze (raw Parquet)  →  Silver (cleaned)  →  Gold (DuckDB + feature tables)
```

- **Bronze**: Raw extracts stored as partitioned Parquet files
- **Silver**: Deduplicated, validated, and normalized data
- **Gold**: DuckDB analytical tables with rolling form, head-to-head, and team summary views

## Documentation

- **[Data Dictionary](docs/data-dictionary.md)** — Complete reference for every table and column across Bronze, Silver, and Gold layers

## Quick Start

```bash
make install-dev   # Install with dev dependencies
make init-db       # Initialize DuckDB schema
make test          # Run tests
make lint          # Lint check
```

## Backfill

Run the backfill script to populate historical data outside of Airflow:

```bash
# Backfill all sports, all configured seasons
uv run python scripts/backfill.py

# Single sport
uv run python scripts/backfill.py --sport soccer

# Specific sport and season
uv run python scripts/backfill.py --sport football --season 2024

# Re-run transform/load on existing bronze data (skip extraction)
uv run python scripts/backfill.py --skip-extract

# Extract + transform only, skip gold feature building
uv run python scripts/backfill.py --skip-gold
```

Available sports: `soccer`, `basketball`, `football`, `all` (default).

## Airflow

Requires Airflow 3.1+ (runs via Docker Compose with PostgreSQL backend):

```bash
make airflow-up    # Start Airflow + Postgres
make airflow-down  # Stop services
```

Access the Airflow UI at `http://localhost:8080` (admin/admin).

DAGs:
- `sports_data_pipeline` — Daily ingestion at 06:00 UTC
- `maintenance_pipeline` — Weekly vacuum on Sundays

## Project Structure

```
src/sports_pipeline/
├── extractors/       # Data extraction (FBref, NBA, NFL)
├── transformers/     # Bronze → Silver transforms
├── loaders/          # DuckDB loader, gold builder, views
├── models/           # Pydantic schemas (bronze/silver/gold)
├── storage/          # Parquet store + path utilities
├── quality/          # Data quality checks
├── utils/            # Logging, rate limiter, retry
├── config.py         # Settings from YAML + env
└── constants.py      # Project constants
```
