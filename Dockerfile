FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock* README.md ./
COPY src/ src/
COPY config/ config/
COPY dags/ dags/
COPY scripts/ scripts/

# Install dependencies
RUN uv sync --frozen --no-dev --extra airflow

# Set Python path
ENV PYTHONPATH=/app/src

ENTRYPOINT ["uv", "run"]
