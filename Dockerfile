FROM python:3.13-slim

RUN pip install uv

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock ./

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN uv sync --frozen --no-install-project --no-dev

COPY src/ src/

EXPOSE 5002

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:5002/api/health').status==200 else 1)"

CMD [".venv/bin/uvicorn", "btc_tracker.dashboard.app:app", "--host", "0.0.0.0", "--port", "5002"]
