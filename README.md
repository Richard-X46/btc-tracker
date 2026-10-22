# Bitcoin On-Chain Dashboard

Live Bitcoin on-chain analytics: blocks, transactions, fee market, and whale activity, served from Databricks with a FastAPI + Plotly frontend. Inspired by [mempool.space](https://mempool.space).

## How it works

- **Source**: blocks and transactions stream from the [AWS Public Blockchain Registry](https://registry.opendata.aws/aws-public-blockchain/) into Delta bronze tables (`workspace.btc_data.blocks`, `transactions`) via incremental batch jobs every 10 minutes. Price comes from Coinbase minute candles (`price_minute`).
- **Transform**: dbt project in `btc_gold/` builds `silver_transactions_flat` (view: fee rates, whale flags) and `gold_daily_fee_metrics` (table: daily fee percentiles, activity).
- **Serve**: `src/btc_tracker/dashboard/app.py` polls the tables on an interval, caches in memory (stale-while-revalidate), and serves Plotly figures plus htmx partials. Times display in Toronto (ET) unless marked UTC.

## Layout

- `src/btc_tracker/dashboard/` FastAPI app + `frontend/` (htmx + Alpine.js, no build step)
- `src/btc_tracker/ingestion/` delta_01 blocks, delta_02 transactions, delta_03 price, delta_health checks
- `src/btc_tracker/spark.py` shared Spark session (UTC pinned, CLI-profile fallback)
- `btc_gold/` dbt project (needs `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`)
- `resources/jobs/` bundle job: `merge_blocks`, `merge_tx`, `sync_price`
- `databricks.yml` asset bundle (dev profile, prod runs as the `btc-tracker-prod` service principal)

## Ingestion

Three scripts in `src/btc_tracker/ingestion/`, scheduled as the `btc-blocks-incremental` job every 10 minutes (tasks run in parallel, merges are idempotent):

- **delta_01_blocks**: reads new date partitions from the public S3 bucket (listed via PyArrow, no full scan), compares the source tip against the local table tip, dedupes by block number keeping the latest `last_modified`, then MERGEs on `number`. A reorg buffer re-winds a few blocks so chain reorgs get overwritten rather than duplicated.
- **delta_02_transactions**: same tip-compare pattern keyed on `block_number`, keeps a flat column subset (drops nested inputs/outputs), MERGEs on `txid`. Backfill mode overwrites a trailing horizon window (default 30 days) partitioned by date.
- **delta_03_btc_price**: pulls Coinbase minute candles (300 per request, 0.4s pacing), MERGEs on `candle_start_ts` so re-pulls update in place. Incremental tops up from the table tip; backfill covers history from the first transaction timestamp.

Each script picks its mode from `INGESTION_MODE` (`incremental` default, `backfill` for history) and emits structured logs/spans to Logfire. `delta_health.py` runs the read-only checks: table listing, sizes, tips, time ranges, column inventory.

## Run locally

```bash
pip install -e .            # or: uv sync
uvicorn btc_tracker.dashboard.app:app --reload   # http://127.0.0.1:8000
```

Auth to Databricks via `DATABRICKS_HOST`/`DATABRICKS_TOKEN` env vars, or a Databricks CLI profile via `DATABRICKS_PROFILE`. Without it, on-chain panels fall back to the local sample in `src/data/sample` (price needs Databricks).

Useful knobs: `DASHBOARD_POLL_INTERVAL` (default 60s), `DASHBOARD_WINDOW_DAYS` (7), `DASHBOARD_SOURCE` (`auto`/`databricks`/`local`), `DASHBOARD_WHALE_BLOCKS` (10).

## Jobs, dbt, deploy

```bash
databricks bundle validate -t prod && databricks bundle deploy -t prod
cd btc_gold && dbt build --profiles-dir .
```

Docker image builds on push to `main` (GHCR, see `.github/workflows/docker-publish.yml`) and serves on port 5002:

```bash
docker build -t btc-tracker .
docker run -p 5002:5002 -e DATABRICKS_HOST=... -e DATABRICKS_TOKEN=... btc-tracker
```
