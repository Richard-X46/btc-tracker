"""Skeleton BTC dashboard.

FastAPI app that serves Plotly figures built from the Databricks tables.

How it works (kept simple on purpose):
- On startup a background poller task refreshes every dataset every
  POLL_INTERVAL seconds and stores the result in an in-memory CACHE dict.
- Endpoints never touch Databricks directly, they just serve CACHE.
- Each refresh retries a few times; if it still fails the last-good data
  is kept and served marked stale, so one bad poll never wipes the page.
  Only a dataset that never loaded answers 503.

Tables used (override with env vars):
- PRICE_TABLE   workspace.btc_data.price_minute
- GOLD_FEE_TABLE workspace.btc_data.gold_daily_fee_metrics
- SILVER_TX_TABLE workspace.btc_data.silver_transactions_flat
"""

from __future__ import annotations

import asyncio
import html
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import date as _date
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import FileResponse, HTMLResponse

from btc_tracker.spark import get_spark

import plotly.graph_objects as go

log = logging.getLogger("btc-dashboard")

# ---------------- config ----------------

PRICE_TABLE = os.getenv("PRICE_TABLE", "workspace.btc_data.price_minute")
GOLD_FEE_TABLE = os.getenv(
    "GOLD_FEE_TABLE", "workspace.btc_data.gold_daily_fee_metrics"
)
SILVER_TX_TABLE = os.getenv(
    "SILVER_TX_TABLE", "workspace.btc_data.silver_transactions_flat"
)
TX_TABLE = os.getenv("TX_TABLE", "workspace.btc_data.transactions")
BLOCKS_TABLE = os.getenv("BLOCKS_TABLE", "workspace.btc_data.blocks")

WINDOW_DAYS = int(os.getenv("DASHBOARD_WINDOW_DAYS", "7"))
WHALE_BLOCKS = int(os.getenv("DASHBOARD_WHALE_BLOCKS", "10"))
WHALE_TXS_PER_BLOCK = int(os.getenv("DASHBOARD_WHALE_TXS_PER_BLOCK", "20"))


def _cutoff(days: int = WINDOW_DAYS) -> str:
    from datetime import timedelta

    return (_date.today() - timedelta(days=days)).isoformat()
POLL_INTERVAL = int(os.getenv("DASHBOARD_POLL_INTERVAL", "60"))  # seconds
REFRESH_RETRIES = int(os.getenv("DASHBOARD_REFRESH_RETRIES", "2"))
RETRY_BACKOFF = float(os.getenv("DASHBOARD_RETRY_BACKOFF", "5"))  # seconds
PRICE_DAYS = int(os.getenv("DASHBOARD_PRICE_DAYS", "7"))
FEE_DAYS = int(os.getenv("DASHBOARD_FEE_DAYS", "7"))
SOURCE = os.getenv("DASHBOARD_SOURCE", "auto")  # auto | databricks | local
LOCAL_DIR = Path(
    os.getenv("LOCAL_SAMPLE_DIR")
    or (Path(__file__).resolve().parents[2] / "data" / "sample")
)
LOCAL_BLOCKS = LOCAL_DIR / "blocks.parquet"
LOCAL_TX = LOCAL_DIR / "transaction_flat.parquet"
ACTIVE = {"source": "databricks"}
# Skip Databricks entirely for this long after a failure, so one wedged
# connection can't pile up hanging threads and starve the server.
DB_COOLDOWN = float(os.getenv("DASHBOARD_DB_COOLDOWN", "300"))
_DB_DOWN_AT = 0.0

# ---------------- time (Toronto-first) ----------------

_TORONTO = None


def _toronto_tz():
    """America/Toronto zone, or None when tzdata is missing (slim images)."""
    global _TORONTO
    if _TORONTO is None:
        try:
            from zoneinfo import ZoneInfo
            _TORONTO = ZoneInfo("America/Toronto")
        except Exception:
            _TORONTO = False
            log.warning("tzdata unavailable, times shown as UTC")
    return _TORONTO or None


def _as_utc(dt) -> datetime | None:
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except ValueError:
            return None
    if dt.tzinfo is None:  # bronze timestamps are UTC without offset
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_time(dt) -> str:
    """Wall time in Toronto (ET suffix), or UTC-labelled fallback."""
    u = _as_utc(dt)
    if u is None:
        return "n/a"
    tz = _toronto_tz()
    if tz is None:
        return u.strftime("%Y-%m-%d %H:%M UTC")
    return u.astimezone(tz).strftime("%Y-%m-%d %H:%M ET")


def _ago_minutes(dt) -> int | None:
    u = _as_utc(dt)
    if u is None:
        return None
    return max(0, int((datetime.now(timezone.utc) - u).total_seconds() // 60))


def ago_text(dt) -> str:
    mins = _ago_minutes(dt)
    if mins is None:
        return "n/a"
    if mins < 60:
        return f"{mins} min ago"
    if mins < 2880:
        return f"{mins // 60} h {mins % 60} min ago"
    return f"{mins // 1440} days ago"

# ---------------- spark (lazy, shared) ----------------

def _db_rows(sql_text: str) -> list[dict]:
    return [r.asDict() for r in get_spark().sql(sql_text).collect()]


def _local_rows(sql_text: str) -> list[dict]:
    import duckdb

    cur = duckdb.sql(sql_text)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _need_local(local_sql: str | None) -> str:
    if local_sql is None:
        raise RuntimeError(
            f"no local sample for this dataset (see {LOCAL_DIR}); "
            "connect Databricks for full data"
        )
    return local_sql


def _q(db_sql: str, local_sql: str | None) -> list[dict]:
    """Run db_sql on Databricks, falling back to local_sql via duckdb."""
    global _DB_DOWN_AT
    if SOURCE == "local":
        ACTIVE["source"] = "local-sample"
        return _local_rows(_need_local(local_sql))
    if time.monotonic() - _DB_DOWN_AT < DB_COOLDOWN:
        ACTIVE["source"] = "local-sample"
        return _local_rows(_need_local(local_sql))
    try:
        rows = _db_rows(db_sql)
        ACTIVE["source"] = "databricks"
        return rows
    except Exception as exc:
        _DB_DOWN_AT = time.monotonic()
        if SOURCE == "databricks":
            raise
        log.warning("databricks query failed, using local sample: %s", exc)
        ACTIVE["source"] = "local-sample"
        return _local_rows(_need_local(local_sql))


# ---------------- fetchers (blocking, run in a thread) ----------------

def fetch_headline() -> dict:
    out: dict[str, Any] = {}
    try:
        last_two = _q(
            f"SELECT close, candle_start_ts FROM {PRICE_TABLE} "
            f"WHERE date >= current_date - 30 "
            "ORDER BY candle_start_ts DESC LIMIT 2",
            None,  # price has no local sample
        )
    except Exception as exc:
        out["price_error"] = str(exc)
    else:
        if last_two:
            out["price"] = last_two[0]["close"]
            out["price_ts"] = str(last_two[0]["candle_start_ts"])
            if len(last_two) == 2 and last_two[1]["close"]:
                prev = last_two[1]["close"]
                out["price_change_1m"] = (last_two[0]["close"] - prev) / prev
    chain = _q(
        f"SELECT date, tx_count, block_count, avg_block_fullness_pct "
        f"FROM {GOLD_FEE_TABLE} ORDER BY date DESC LIMIT 1",
        f"""SELECT t.date AS date, t.n AS tx_count, b.n AS block_count,
                   b.full AS avg_block_fullness_pct
            FROM (SELECT date, COUNT(*) AS n FROM '{LOCAL_TX}'
                  GROUP BY date ORDER BY date DESC LIMIT 1) t
            JOIN (SELECT date, COUNT(*) AS n,
                         AVG(weight / 4000000.0 * 100) AS full
                  FROM '{LOCAL_BLOCKS}'
                  GROUP BY date ORDER BY date DESC LIMIT 1) b
              ON t.date = b.date""",
    )
    if chain:
        out.update({k: chain[0][k] for k in
                    ("date", "tx_count", "block_count",
                     "avg_block_fullness_pct")})
    tip = _q(
        # date filter: partition-pruned read, avoids WHOLE_TABLE_READ
        # conflicts with the concurrent 10-min merge
        f"SELECT max(number) AS tip_number, max(timestamp) AS tip_ts "
        f"FROM {BLOCKS_TABLE} WHERE date >= current_date - 7",
        f"SELECT max(number) AS tip_number, max(timestamp) AS tip_ts "
        f"FROM '{LOCAL_BLOCKS}'",
    )
    if tip:
        out["tip_number"] = tip[0]["tip_number"]
        out["tip_ts"] = fmt_time(tip[0]["tip_ts"])
        out["tip_ago"] = ago_text(tip[0]["tip_ts"])
        out["tip_ago_minutes"] = _ago_minutes(tip[0]["tip_ts"])
    return out


def fetch_price_daily(days: int = PRICE_DAYS) -> list[dict]:
    return _q(
        f"""WITH d AS (
              SELECT date AS day, MAX(high) AS high, MIN(low) AS low,
                     SUM(volume) AS volume, MAX(candle_start_ts) AS last_ts
              FROM {PRICE_TABLE}
              WHERE date >= date_format(current_date - {days}, 'yyyy-MM-dd')
              GROUP BY date
            )
            SELECT d.day, d.high, d.low, d.volume, p.close AS close
            FROM d JOIN {PRICE_TABLE} p ON p.candle_start_ts = d.last_ts
            ORDER BY d.day""",
        None,  # price has no local sample
    )


def fetch_fees(days: int = FEE_DAYS) -> list[dict]:
    return _q(
        f"SELECT date, p50_sat_vbyte, p90_sat_vbyte, p99_sat_vbyte, tx_count "
        f"FROM {GOLD_FEE_TABLE} "
        f"WHERE date >= date_sub((SELECT max(date) FROM {GOLD_FEE_TABLE}), {days}) "
        f"ORDER BY date",
        f"""SELECT date,
                   quantile_cont((fee * 100000000) / NULLIF(virtual_size, 0), 0.5)
                     AS p50_sat_vbyte,
                   quantile_cont((fee * 100000000) / NULLIF(virtual_size, 0), 0.9)
                     AS p90_sat_vbyte,
                   quantile_cont((fee * 100000000) / NULLIF(virtual_size, 0), 0.99)
                     AS p99_sat_vbyte,
                   COUNT(*) AS tx_count
            FROM '{LOCAL_TX}' WHERE NOT is_coinbase
              AND date >= (SELECT (max(date)::DATE - INTERVAL {days} DAY)::VARCHAR
                           FROM '{LOCAL_TX}')
            GROUP BY date ORDER BY date""",
    )


def fetch_activity(days: int = FEE_DAYS) -> list[dict]:
    return _q(
        f"SELECT date, block_count, tx_count, avg_block_fullness_pct "
        f"FROM {GOLD_FEE_TABLE} "
        f"WHERE date >= date_sub((SELECT max(date) FROM {GOLD_FEE_TABLE}), {days}) "
        f"ORDER BY date",
        f"""SELECT b.date AS date, b.n AS block_count, t.n AS tx_count,
                   b.full AS avg_block_fullness_pct
            FROM (SELECT date, COUNT(*) AS n,
                         AVG(weight / 4000000.0 * 100) AS full
                  FROM '{LOCAL_BLOCKS}' GROUP BY date) b
            JOIN (SELECT date, COUNT(*) AS n FROM '{LOCAL_TX}'
                  GROUP BY date) t USING (date)
            WHERE b.date >= (SELECT (max(date)::DATE - INTERVAL {days} DAY)::VARCHAR
                             FROM '{LOCAL_BLOCKS}')
            ORDER BY b.date""",
    )


FEE_BANDS = ["0-2", "2-10", "10-50", "50+"]


def fetch_fee_bands() -> list[dict]:
    """Long-format {date, band, n} fee-rate distribution per day."""
    cutoff = _cutoff()

    def sql(src: str) -> str:
        return f"""SELECT date,
                   CASE WHEN spb < 2 THEN '0-2'
                        WHEN spb < 10 THEN '2-10'
                        WHEN spb < 50 THEN '10-50'
                        ELSE '50+' END AS band,
                   COUNT(*) AS n
            FROM (SELECT date, (fee * 100000000) / NULLIF(virtual_size, 0) AS spb
                  FROM {src}
                  WHERE NOT is_coinbase AND date >= '{cutoff}')
            GROUP BY date, band ORDER BY date"""

    return _q(sql(TX_TABLE), sql(f"'{LOCAL_TX}'"))


def fetch_block_fullness(limit: int = 250) -> list[dict]:
    """Per-block fullness for the most recent blocks."""
    limit = max(50, min(limit, 1000))
    # date filter on the live query only: partition-pruned read, avoids
    # WHOLE_TABLE_READ conflicts with the concurrent 10-min merge.
    # (Local sample keeps no filter so the stale demo file still renders.)
    db_sql = (f"SELECT number, weight / 4000000.0 * 100 AS fullness, "
              f"transaction_count AS txs FROM {BLOCKS_TABLE} "
              f"WHERE date >= current_date - 30 "
              f"ORDER BY number DESC LIMIT {limit}")
    local_sql = (f"SELECT number, weight / 4000000.0 * 100 AS fullness, "
                 f"transaction_count AS txs FROM '{LOCAL_BLOCKS}' "
                 f"ORDER BY number DESC LIMIT {limit}")
    return _q(db_sql, local_sql)[::-1]


def fetch_tx_heatmap() -> list[dict]:
    """Hourly transaction counts {hr (datetime), n} for the window."""
    cutoff = _cutoff()

    def sql(src: str) -> str:
        return (f"SELECT date_trunc('hour', block_timestamp) AS hr, "
                f"COUNT(*) AS n FROM {src} "
                f"WHERE date >= '{cutoff}' GROUP BY hr ORDER BY hr")

    return _q(sql(TX_TABLE), sql(f"'{LOCAL_TX}'"))


def fetch_pulse(days: int = WINDOW_DAYS) -> dict:
    """Price + chain activity over the last `days` of available data.

    Hourly grain (168 points for 7d). Window is anchored to max() in each
    table so it stays populated even if the pipeline lags wall-clock time.
    """
    price = _q(
        f"""WITH h AS (
              SELECT date_trunc('hour', candle_start_ts) AS hr,
                     MAX(candle_start_ts) AS last_ts
              FROM {PRICE_TABLE}
              WHERE candle_start_ts >=
                (SELECT max(candle_start_ts) FROM {PRICE_TABLE}
                 WHERE date >= current_date - 30)
                - INTERVAL {days} DAYS
              GROUP BY 1
            )
            SELECT h.hr AS ts, p.close AS close
            FROM h JOIN {PRICE_TABLE} p ON p.candle_start_ts = h.last_ts
            ORDER BY ts""",
        None,  # no local price sample
    )
    chain = _q(
        f"""SELECT date_trunc('hour', block_timestamp) AS w, COUNT(*) AS n,
                   percentile_approx((fee * 100000000) / NULLIF(virtual_size, 0), 0.5)
                     AS med_fee
            FROM {TX_TABLE}
            WHERE NOT is_coinbase
              AND block_timestamp >=
                (SELECT max(block_timestamp) FROM {TX_TABLE}
                 WHERE date >= current_date - 30) - INTERVAL {days} DAYS
            GROUP BY 1 ORDER BY w""",
        f"""SELECT date_trunc('hour', block_timestamp) AS w, COUNT(*) AS n,
                   quantile_cont((fee * 100000000) / NULLIF(virtual_size, 0), 0.5)
                     AS med_fee
            FROM '{LOCAL_TX}'
            WHERE NOT is_coinbase
              AND block_timestamp >=
                (SELECT max(block_timestamp) FROM '{LOCAL_TX}') - INTERVAL {days} DAYS
            GROUP BY 1 ORDER BY w""",
    )
    return {"price": price, "chain": chain}


def fetch_whale_blocks(n_blocks: int = WHALE_BLOCKS,
                       per_block: int = WHALE_TXS_PER_BLOCK) -> dict:
    """Last N blocks with whale activity: aggregates + top txs per block."""
    n_blocks = max(1, min(n_blocks, 50))
    per_block = max(1, min(per_block, 100))

    blocks = _q(
        f"SELECT block_number, MAX(block_timestamp) AS ts, "
        f"COUNT(*) AS n, SUM(output_value) AS vol FROM {TX_TABLE} "
        f"WHERE output_value > 1.0 AND date >= current_date - 30 "
        f"GROUP BY block_number ORDER BY block_number DESC "
        f"LIMIT {n_blocks}",
        f"SELECT block_number, MAX(block_timestamp) AS ts, "
        f"COUNT(*) AS n, SUM(output_value) AS vol FROM '{LOCAL_TX}' "
        f"WHERE output_value > 1.0 "
        f"GROUP BY block_number ORDER BY block_number DESC "
        f"LIMIT {n_blocks}",
    )
    if not blocks:
        return {"blocks": [], "total_n": 0, "total_vol": 0.0}
    nums = ",".join(str(int(b["block_number"])) for b in blocks)

    def txs_sql(src: str) -> str:
        return (f"""SELECT block_number, txid, output_value, fee,
                           (fee * 100000000) / NULLIF(virtual_size, 0)
                             AS sat_per_vbyte,
                           block_timestamp FROM (
                             SELECT t.*, ROW_NUMBER() OVER (
                               PARTITION BY block_number
                               ORDER BY output_value DESC) AS rn
                             FROM {src} t
                             WHERE output_value > 1.0
                               AND block_number IN ({nums}))
                    WHERE rn <= {per_block}
                    ORDER BY block_number DESC, output_value DESC""")

    txs = _q(txs_sql(TX_TABLE), txs_sql(f"'{LOCAL_TX}'"))
    by_block: dict[int, list[dict]] = {}
    for t in txs:
        by_block.setdefault(int(t["block_number"]), []).append(t)
    for b in blocks:
        b["ts"] = fmt_time(b["ts"])
        b["txs"] = by_block.get(int(b["block_number"]), [])
        for t in b["txs"]:
            t["block_timestamp"] = fmt_time(t["block_timestamp"])
    return {"blocks": blocks,
            "total_n": sum(int(b["n"]) for b in blocks),
            "total_vol": sum(float(b["vol"] or 0) for b in blocks)}


def fetch_whales(limit: int = 50) -> list[dict]:
    limit = max(1, min(limit, 200))
    return _q(
        f"SELECT txid, block_number, output_value, fee, sat_per_vbyte, "
        f"block_timestamp FROM {SILVER_TX_TABLE} "
        f"WHERE is_whale = true AND date >= current_date - 30 "
        f"ORDER BY block_timestamp DESC LIMIT {limit}",
        f"""SELECT txid, block_number, output_value, fee,
                   (fee * 100000000) / NULLIF(virtual_size, 0)
                     AS sat_per_vbyte,
                   block_timestamp FROM '{LOCAL_TX}'
            WHERE output_value > 1.0
            ORDER BY block_timestamp DESC LIMIT {limit}""",
    )


FETCHERS = {
    "headline": fetch_headline,
    "price_daily": fetch_price_daily,
    "fees": fetch_fees,
    "activity": fetch_activity,
    "fee_bands": fetch_fee_bands,
    "block_fullness": fetch_block_fullness,
    "tx_heatmap": fetch_tx_heatmap,
    "pulse": fetch_pulse,
    "whale_blocks": fetch_whale_blocks,
    "whales": fetch_whales,
}

# ---------------- cache + poller ----------------

CACHE: dict[str, dict[str, Any]] = {}  # key -> {"asof": ts, "data"|"error"}


def _refresh_one(key: str) -> None:
    """Refresh one dataset with retries; on failure keep last-good as stale."""
    last_exc: Exception | None = None
    for attempt in range(REFRESH_RETRIES + 1):
        try:
            CACHE[key] = {"asof": time.time(), "checked": time.time(),
                          "data": FETCHERS[key](),
                          "source": ACTIVE["source"], "stale": False,
                          "error": None}
            log.info("dashboard cache refreshed: %s (%s)", key, ACTIVE["source"])
            return
        except Exception as exc:  # noqa: BLE001 - surfaced via /api/health
            last_exc = exc
            log.warning("dashboard refresh failed [%s] attempt %d: %s",
                        key, attempt + 1, exc)
            time.sleep(RETRY_BACKOFF)
    prev = CACHE.get(key) or {}
    entry: dict[str, Any] = {"asof": prev.get("asof"), "checked": time.time(),
                             "source": ACTIVE["source"], "stale": True,
                             "error": str(last_exc)}
    if "data" in prev:  # keep serving last-good data, flagged stale
        entry["data"] = prev["data"]
        entry["asof"] = prev.get("asof")
    CACHE[key] = entry


async def _poller() -> None:
    while True:
        for key in FETCHERS:
            await asyncio.to_thread(_refresh_one, key)
        await asyncio.sleep(POLL_INTERVAL)


def cached(key: str) -> Any:
    """Return cached data (stale if the last refresh failed).

    Raises 503 only when the dataset never loaded at all.
    """
    from fastapi import HTTPException

    entry = CACHE.get(key)
    if not entry or "data" not in entry:
        raise HTTPException(
            503, f"'{key}' not loaded yet: {(entry or {}).get('error', 'poller warming up')}")
    return entry["data"]


def is_stale(key: str) -> bool:
    return bool((CACHE.get(key) or {}).get("stale"))


def cache_asof(key: str) -> float | None:
    entry = CACHE.get(key)
    return entry["asof"] if entry else None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup must NEVER block on data: the poller refreshes immediately
    # on its first iteration, and endpoints 503-until-warm by design.
    # (Blocking warmup here kept uvicorn from accepting traffic while
    # Databricks was unreachable, surfacing as empty responses.)
    task = asyncio.create_task(_poller())
    yield
    task.cancel()


app = FastAPI(title="Bitcoin On-Chain Dashboard", lifespan=lifespan)


# ---------------- figures ----------------

def _themed(fig: go.Figure, title: str) -> dict:
    fig.update_layout(title=title, template="plotly_white",
                      height=420, margin={"t": 50, "b": 40, "l": 50, "r": 50},
                      paper_bgcolor="rgba(0,0,0,0)",
                      plot_bgcolor="rgba(0,0,0,0)")
    return fig.to_plotly_json()


def price_figure(rows: list[dict]) -> dict:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=[r["day"] for r in rows], y=[r["close"] for r in rows],
        mode="lines+markers", name="close", line={"color": "#f7931a"}))
    fig.add_trace(go.Bar(
        x=[r["day"] for r in rows], y=[r["volume"] for r in rows],
        name="volume", yaxis="y2", opacity=0.3,
        marker_color="#4a5568"))
    fig.update_layout(
        yaxis_title="USD",
        yaxis2={"title": "volume", "overlaying": "y", "side": "right",
                "showgrid": False})
    return _themed(fig, "BTC daily close + volume")


def fees_figure(rows: list[dict]) -> dict:
    fig = go.Figure()
    for col, name, color in (("p50_sat_vbyte", "p50", "#2f855a"),
                             ("p90_sat_vbyte", "p90", "#b7791f"),
                             ("p99_sat_vbyte", "p99", "#c53030")):
        fig.add_trace(go.Scatter(
            x=[r["date"] for r in rows], y=[r[col] for r in rows],
            mode="lines", name=name, line={"color": color}))
    fig.update_layout(yaxis_title="sat/vbyte")
    return _themed(fig, "Fee market (sat/vbyte)")


def activity_figure(rows: list[dict]) -> dict:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=[r["date"] for r in rows], y=[r["tx_count"] for r in rows],
        name="tx_count", marker_color="#3182ce"))
    fig.update_layout(yaxis_title="tx count", showlegend=False)
    return _themed(fig, "Chain activity")


def pulse_figure(pulse: dict) -> dict:
    """Price vs chain activity, last 7d: USD left, tx-rate + fee right."""
    price, chain = pulse.get("price", []), pulse.get("chain", [])
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=[r["ts"] for r in price], y=[r["close"] for r in price],
        mode="lines", name="BTC USD", line={"color": "#dd6b20", "width": 2}))
    fig.add_trace(go.Bar(
        x=[r["w"] for r in chain], y=[r["n"] for r in chain],
        name="tx / hour", yaxis="y2", opacity=0.35,
        marker_color="#93c5fd"))
    fig.add_trace(go.Scatter(
        x=[r["w"] for r in chain], y=[r["med_fee"] for r in chain],
        mode="lines", name="median sat/vB", yaxis="y3",
        line={"color": "#c53030", "dash": "dash"}))
    fig.update_layout(
        yaxis={"title": "BTC USD"},
        yaxis2={"title": "tx / hour", "overlaying": "y", "side": "right",
                "showgrid": False},
        yaxis3={"title": "median sat/vB", "overlaying": "y", "side": "right",
                "anchor": "free", "position": 0.92, "showgrid": False})
    if not price and not chain:
        fig.add_annotation(text="no data in window", showarrow=False)
    out = _themed(fig, "Price vs network pulse (last 7 days)")
    out["layout"].setdefault("xaxis", {})["domain"] = [0, 0.88]
    out["layout"]["margin"]["r"] = 90
    return out


# ---------------- api ----------------

@app.get("/api/health")
def health() -> dict:
    return {
        key: {"asof": cache_asof(key),
              "ok": "data" in (CACHE.get(key) or {}),
              "stale": is_stale(key),
              "source": (CACHE.get(key) or {}).get("source"),
              "error": (CACHE.get(key) or {}).get("error")}
        for key in FETCHERS
    }


@app.get("/api/headline")
def headline() -> dict:
    return {"asof": cache_asof("headline"), "stale": is_stale("headline"),
            "data": cached("headline")}


@app.get("/api/figures/price")
def figure_price() -> dict:
    return {"asof": cache_asof("price_daily"), "stale": is_stale("price_daily"),
            "figure": price_figure(cached("price_daily"))}


@app.get("/api/figures/fees")
def figure_fees() -> dict:
    return {"asof": cache_asof("fees"), "stale": is_stale("fees"),
            "figure": fees_figure(cached("fees"))}


@app.get("/api/figures/activity")
def figure_activity() -> dict:
    return {"asof": cache_asof("activity"), "stale": is_stale("activity"),
            "figure": activity_figure(cached("activity"))}


@app.get("/api/figures/pulse")
def figure_pulse() -> dict:
    return {"asof": cache_asof("pulse"), "stale": is_stale("pulse"),
            "figure": pulse_figure(cached("pulse"))}


@app.get("/api/whales")
def whales(limit: int = 50) -> dict:
    rows = cached("whales")
    return {"asof": cache_asof("whales"), "stale": is_stale("whales"),
            "data": rows[: max(1, min(limit, 200))]}


# ---------------- page + htmx partials ----------------

FRONTEND_DIR = Path(__file__).parent / "frontend"


@app.get("/", response_class=HTMLResponse)
def index() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


def _err_html(what: str) -> str:
    entry = CACHE.get(what, {})
    return f"<span class=err>'{what}': {html.escape(str(entry.get('error', 'warming up…')))}</span>"


@app.get("/partials/headline", response_class=HTMLResponse)
def partial_headline() -> str:
    entry = CACHE.get("headline")
    if not entry or "data" not in entry:
        return _err_html("headline")
    d = entry["data"]
    parts = []
    if isinstance(d.get("price"), (int, float)):
        chg = d.get("price_change_1m")
        chg_txt = f"{chg:+.2%}" if isinstance(chg, (int, float)) else "n/a"
        parts.append(f"<strong>${d['price']:,.0f}</strong> ({chg_txt})")
    elif d.get("price_error"):
        parts.append(f"<em>price n/a</em>")
    if d.get("date"):
        try:
            day = datetime.fromisoformat(str(d["date"])).strftime("%b %d")
        except ValueError:
            day = str(d["date"])
        today_utc = datetime.now(timezone.utc).date().isoformat()
        if str(d["date"]) >= today_utc:
            day += " (today, partial)"
        parts.append(day)
    if d.get("tx_count") is not None:
        parts.append(f"tx: <strong>{d['tx_count']:,}</strong>")
    if d.get("block_count") is not None:
        parts.append(f"blocks: <strong>{d['block_count']}</strong>")
    if isinstance(d.get("avg_block_fullness_pct"), (int, float)):
        parts.append(f"fullness: <strong>{d['avg_block_fullness_pct']:.1f}%</strong>")
    if d.get("tip_number") is not None:
        tip_txt = (f"last block <strong>#{d['tip_number']:,}</strong> added "
                   f"<strong>{html.escape(str(d.get('tip_ago', 'n/a')))}</strong>")
        if d.get("tip_ts"):
            tip_txt += f" <small>{html.escape(str(d['tip_ts']))}</small>"
        parts.append(tip_txt)
    parts.append(f"<small>(source: {html.escape(str(entry.get('source')))})</small>")
    if entry.get("stale"):
        parts.append("<small class=err>(stale)</small>")
    return " &nbsp;·&nbsp; ".join(parts) if len(parts) > 1 else (parts[0] if parts else "no data")


def _whale_tx_row(t: dict) -> str:
    spb = t.get("sat_per_vbyte")
    spb_txt = f"{float(spb):,.1f}" if spb is not None else "n/a"
    return (
        f"<tr><td><code>{html.escape(str(t.get('txid', ''))[:16])}…</code></td>"
        f"<td>{t.get('output_value')}</td>"
        f"<td>{t.get('fee')}</td>"
        f"<td>{spb_txt}</td>"
        f"<td>{html.escape(str(t.get('block_timestamp', '')))}</td></tr>"
    )


@app.get("/partials/whale-blocks", response_class=HTMLResponse)
def partial_whale_blocks() -> str:
    entry = CACHE.get("whale_blocks")
    if not entry or "data" not in entry:
        return _err_html("whale_blocks")
    d = entry["data"]
    blocks = d["blocks"]
    if not blocks:
        return "<em>no whale activity in window</em>"
    maxn = max(int(b["n"]) for b in blocks) or 1
    head = (f"<strong>{d['total_n']:,}</strong> whale txs · "
            f"<strong>{d['total_vol']:,.1f} BTC</strong> moved across "
            f"last <strong>{len(blocks)}</strong> blocks with whale activity")
    if entry.get("stale"):
        head += " <small class=err>(stale)</small>"
    dist = "".join(
        f"<div class=dist-row><span>#{b['block_number']}</span>"
        f"<div class=dist-bar style='width:{100 * int(b['n']) / maxn:.0f}%'></div>"
        f"<span>{int(b['n']):,}</span></div>"
        for b in blocks
    )
    det = "".join(
        f"<details><summary>Block {b['block_number']} "
        f"<small>{int(b['n']):,} whale txs · "
        f"{float(b['vol'] or 0):,.1f} BTC · "
        f"{html.escape(str(b['ts']))}</small></summary>"
        f"<table><thead><tr><th>txid</th><th>output BTC</th>"
        f"<th>fee</th><th>sat/vB</th><th>time</th></tr></thead><tbody>"
        + "".join(_whale_tx_row(t) for t in b["txs"])
        + "</tbody></table></details>"
        for b in blocks
    )
    return f"{head}<div class=dist>{dist}</div>{det}"
