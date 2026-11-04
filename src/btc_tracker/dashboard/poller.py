"""Live gold puller: event-driven refresh of live tables via open sharing.

Nothing here runs on a timer. Flow:
  mempool block -> poll_s3 gate -> trigger_job -> run_id
  -> wait_run(run_id) -> SUCCESS -> refresh_live()
Readers serve cache or 503; a failed refresh keeps last-good + flags stale.
"""
import asyncio

import httpx
import logfire

from btc_tracker.dashboard.cache import cache, now_utc
from btc_tracker.dashboard.share import query_share
from btc_tracker.dashboard.db_trigger import HOST, TOKEN

DAILY_TABLES = ["gold_daily_fee_metrics", "gold_daily_miner_revenue", "gold_daily_whale_activity"]
WEEKLY_TABLES = ["gold_weekly_fee_heatmap"]
INITIAL_WAIT_S = 60
POLL_S = 30
TIMEOUT_S = 600


def _live_specs():
    # slot -> (share table, sql, hints). Slot and table differ for price_24h:
    # there is no share table by that name; it reads price_minute.
    return {
        "live_block_feed": (
            "live_block_feed",
            "select * from read_parquet(?, union_by_name=True) order by number",
            None,
        ),
        "price_24h": (
            "price_minute",
            "select candle_start_ts as ts, open, high, low, close, volume"
            " from read_parquet(?, union_by_name=True)"
            " where candle_start_ts >= now() - INTERVAL '3 days' order by 1",
            None,
        ),
    }


async def _pull(table: str, sql: str, hints=None, tries: int = 5):
    """query_share with retries (price_minute listing flaps: back off and hold)."""
    last = None
    for i in range(tries):
        try:
            return await asyncio.to_thread(query_share, table, sql, hints)
        except Exception as e:
            last = e
            await asyncio.sleep(5 * (i + 1))
    raise last
    """query_share with one retry (share metadata flaps at boot)."""
    last = None
    for _ in range(tries):
        try:
            return await asyncio.to_thread(query_share, table, sql, hints)
        except Exception as e:
            last = e
            await asyncio.sleep(5)
    raise last


async def refresh_live():
    for slot_name, (table, sql, hints) in _live_specs().items():
        slot = cache["live"][slot_name]
        try:
            df = await _pull(table, sql, hints)
            slot.update(df=df, asof=now_utc(), ok=True, stale=False, error=None)
            logfire.info("live.refresh.ok", table=table, rows=df.height)
        except Exception as e:
            slot.update(ok=False, stale=slot["df"] is not None, error=str(e)[:200])
            logfire.warn("live.refresh.fail", table=table, error=str(e)[:200])


async def refresh_daily(force: bool = False):
    """Pull daily golds when the slot is empty or the UTC date rolled."""
    today = now_utc().date().isoformat()

    async def one(table: str):
        slot = cache["daily"][table]
        asof = slot["asof"]
        if not force and slot["df"] is not None and asof is not None:
            if asof.date().isoformat() >= today:
                return
        try:
            df = await _pull(
                table,
                "select * from read_parquet(?, union_by_name=True) order by date",
            )
            slot.update(df=df, asof=now_utc(), ok=True, stale=False, error=None)
            logfire.info("daily.refresh.ok", table=table, rows=df.height)
        except Exception as e:
            slot.update(ok=False, stale=slot["df"] is not None, error=str(e)[:200])
            logfire.warn("daily.refresh.fail", table=table, error=str(e)[:200])

    await asyncio.gather(*(one(t) for t in DAILY_TABLES))
    await _refresh_price(force=force)


def _fetch_price():
    """Coinbase BTC-USD daily since 2015 (granularity=86400, 300/request).

    Same source family as the minutely feed. Memory only, no table.
    """
    import datetime as _dt
    import time as _time

    import httpx
    import polars as pl

    rows = []
    start = _dt.datetime(2015, 1, 1, tzinfo=_dt.timezone.utc)
    now = _dt.datetime.now(_dt.timezone.utc)
    with httpx.Client(timeout=30) as client:
        while start < now:
            end = min(start + _dt.timedelta(days=300), now)
            r = client.get(
                "https://api.exchange.coinbase.com/products/BTC-USD/candles",
                params={"start": start.isoformat(), "end": end.isoformat(),
                        "granularity": 86400},
            )
            r.raise_for_status()
            for t, low, high, o, c, v in r.json():
                ts = _dt.datetime.fromtimestamp(t, tz=_dt.timezone.utc)
                rows.append({"date": ts.date().isoformat(), "close": c, "volume": v})
            start = end
            _time.sleep(0.2)
    df = pl.DataFrame(rows).unique("date").sort("date")
    return df.with_columns(pl.col("date").str.to_date())


async def _refresh_price(force: bool = False):
    slot = cache["daily"]["price_daily"]
    today = now_utc().date().isoformat()
    asof = slot["asof"]
    if not force and slot["df"] is not None and asof is not None:
        if asof.date().isoformat() >= today:
            return
    try:
        df = await asyncio.to_thread(_fetch_price)
        slot.update(df=df, asof=now_utc(), ok=True, stale=False, error=None)
        logfire.info("daily.refresh.ok", table="price_daily", rows=df.height)
    except Exception as e:
        slot.update(ok=False, stale=slot["df"] is not None, error=str(e)[:200])
        logfire.warn("daily.refresh.fail", table="price_daily", error=str(e)[:200])


async def refresh_weekly(force: bool = False):
    """Pull weekly golds when the slot is empty or a new week rolled."""
    import datetime as _dt

    today = now_utc().date()
    monday = today - _dt.timedelta(days=today.weekday())

    async def one(table: str):
        slot = cache["weekly"][table]
        asof = slot["asof"]
        if not force and slot["df"] is not None and asof is not None:
            if asof.date() >= monday:
                return
        try:
            df = await _pull(
                table,
                "select * from read_parquet(?, union_by_name=True)",
            )
            slot.update(df=df, asof=now_utc(), ok=True, stale=False, error=None)
            logfire.info("weekly.refresh.ok", table=table, rows=df.height)
        except Exception as e:
            slot.update(ok=False, stale=slot["df"] is not None, error=str(e)[:200])
            logfire.warn("weekly.refresh.fail", table=table, error=str(e)[:200])

    await asyncio.gather(*(one(t) for t in WEEKLY_TABLES))


async def wait_run(run_id: int):
    """Wait for a Databricks run, then refresh live on success."""
    if not HOST or not TOKEN:
        logfire.warn("live.wait.skip", reason="no warehouse creds")
        return
    url = f"{HOST}/api/2.1/jobs/runs/get"
    # Job needs ~1-4 min; don't burn polls while it spins up.
    await asyncio.sleep(INITIAL_WAIT_S)
    waited = INITIAL_WAIT_S
    async with httpx.AsyncClient(timeout=30) as client:
        while waited < TIMEOUT_S:
            try:
                r = await client.get(
                    url,
                    headers={"Authorization": f"Bearer {TOKEN}"},
                    params={"run_id": run_id},
                )
                r.raise_for_status()
                body = r.json()
                st = body.get("status", {}) or {}
                life = st.get("state")
                code = (st.get("termination_details") or {}).get("code")
            except Exception as e:
                logfire.warn("live.wait.poll_fail", error=str(e)[:200])
                life, code = None, None
            if life == "TERMINATED":
                if code == "SUCCESS":
                    await refresh_live()
                else:
                    logfire.warn("live.wait.bad_result", code=code)
                return
            await asyncio.sleep(POLL_S)
            waited += POLL_S
    logfire.warn("live.wait.timeout", run_id=run_id)
