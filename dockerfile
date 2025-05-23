# Use Python 3.12‐slim as the base image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy dependency files first (for layer caching)
COPY pyproject.toml uv.lock* ./

# Install UV, Prefect CLI, and any pinned deps
RUN pip install --no-cache-dir uv prefect

# Install your project in “editable” mode
RUN uv pip install --system -e .

# Copy application code and entrypoint helper script
COPY . .
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Entrypoint will dispatch based on the first arg (“app” or “agent”)
ENTRYPOINT ["/entrypoint.sh"]
CMD ["app"]