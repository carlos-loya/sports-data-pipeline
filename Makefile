.PHONY: install install-dev test test-cov lint format check init-db backfill airflow-up airflow-down

install:
	uv sync

install-dev:
	uv sync --extra dev

test:
	uv run pytest

test-cov:
	uv run pytest --cov=src/sports_pipeline --cov-report=html

lint:
	uv run ruff check src/ tests/

format:
	uv run ruff format src/ tests/

check: lint test

init-db:
	uv run python scripts/init_duckdb.py

backfill:
	uv run python scripts/backfill.py

airflow-up:
	docker compose up -d

airflow-down:
	docker compose down
