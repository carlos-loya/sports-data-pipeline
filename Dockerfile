FROM python:3.11-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock* ./
COPY src/ src/
COPY config/ config/
COPY dags/ dags/
COPY scripts/ scripts/

# Install dependencies
RUN uv sync --frozen --no-dev

# Set Python path
ENV PYTHONPATH=/app/src

ENTRYPOINT ["uv", "run"]
