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

## Quick Start

```bash
make install-dev   # Install with dev dependencies
make init-db       # Initialize DuckDB schema
make test          # Run tests
make lint          # Lint check
```

## Airflow

```bash
make airflow-up    # Start Airflow + Postgres
make airflow-down  # Stop services
```

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
