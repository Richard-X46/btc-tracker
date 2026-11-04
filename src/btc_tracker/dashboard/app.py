"""Bitcoin On-Chain Dashboard API - clean slate."""

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from btc_tracker.dashboard.cache import cache
from btc_tracker.dashboard.watcher import listen_mempool
from btc_tracker.dashboard.s3_poller import poll_s3
from btc_tracker.dashboard.db_trigger import trigger_job
from btc_tracker.dashboard.poller import wait_run, refresh_live, refresh_daily, refresh_weekly
from btc_tracker.dashboard.figures import build_pace, build_last_n, build_fees, build_security, build_whale, build_footprint, build_heatmap, build_tape, build_price
from btc_tracker.dashboard.style import apply_house_style
import asyncio
import os
import time
from contextlib import asynccontextmanager

import logfire

async def _track(height, ts):
    try:
        run_id = await poll_s3(height, ts, trigger_job)
    except Exception as e:
        print(f"track {height}: trigger path failed: {type(e).__name__}: {e}")
        return
    if run_id:
        print(f"track {height}: triggered run {run_id}, waiting")
        await wait_run(run_id)
    else:
        print(f"track {height}: no trigger (already covered or S3 miss)")


async def _prime():
    await refresh_live()
    await refresh_daily()
    await refresh_weekly()


async def _live_loop():
    while True:
        await asyncio.sleep(300)
        await refresh_live()


async def handle_tip(height, data):
    if height is not None:
        with logfire.span("tip.emit", height=height):
            cache["tip"] = height
            if data.get("block") is not None:
                blk = data["block"]
                cache["blocks"] = [b for b in cache["blocks"] if b.get("height") != blk.get("height")] + [blk]
                cache["blocks"] = sorted(cache["blocks"], key=lambda b: b.get("height", 0))[-100:]
                ts = blk.get("timestamp") or time.time()
                asyncio.create_task(_track(height, ts), name=f"s3poll-{height}")
    if data.get("mempool-blocks") is not None:
        cache["mempool"] = data["mempool-blocks"]
    if data.get('blocks') is not None:
        merged = {b.get("height"): b for b in cache["blocks"]}
        merged.update({b.get("height"): b for b in data['blocks']})
        cache["blocks"] = sorted(merged.values(), key=lambda b: b.get("height", 0))[-100:]
        if cache["tip"] is None and cache["blocks"]:
            cache["tip"] = cache["blocks"][-1].get("height")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.getenv("LOGFIRE_TOKEN"):
        logfire.configure(service_name="btc-trigger")
    apply_house_style()
    asyncio.create_task(_prime(), name="prime-all")
    asyncio.create_task(_live_loop(), name="live-fallback")
    t = asyncio.create_task(listen_mempool(handle_tip), name="mempool-watcher")
    yield
    t.cancel()

app = FastAPI(title="Bitcoin On-Chain Dashboard",lifespan=lifespan)

FRONTEND = os.path.join(os.path.dirname(__file__), "frontend", "index.html")


@app.get("/")
def dashboard() -> FileResponse:
    return FileResponse(FRONTEND, media_type="text/html")

@app.get("/api/tip")
def get_tip() -> dict:
    return {"tip": cache["tip"]}

@app.get("/api/mempool")
def get_mempool() -> dict:
    return {"mempool": cache["mempool"]}

@app.get("/api/blocks")
def get_blocks() -> dict:
    return {"blocks": cache["blocks"]}


@app.get("/api/figures/pace")
def pace_figure() -> dict:
    slot = cache["live"]["live_block_feed"]
    if slot["df"] is None:
        raise HTTPException(503, "live cache warming")
    return {"figure": build_pace(slot["df"])}


@app.get("/api/figures/last-blocks")
def last_blocks_figure(n: int = 10) -> dict:
    slot = cache["live"]["live_block_feed"]
    if slot["df"] is None:
        raise HTTPException(503, "live cache warming")
    return {"figure": build_last_n(slot["df"], n=min(max(n, 1), 120))}


@app.get("/api/figures/fees")
def fees_figure() -> dict:
    slot = cache["daily"]["gold_daily_fee_metrics"]
    if slot["df"] is None:
        raise HTTPException(503, "daily cache warming")
    try:
        return {"figure": build_fees(slot["df"])}
    except ValueError as e:
        raise HTTPException(503, str(e))


@app.get("/api/figures/security")
def security_figure() -> dict:
    slot = cache["daily"]["gold_daily_miner_revenue"]
    if slot["df"] is None:
        raise HTTPException(503, "daily cache warming")
    try:
        return {"figure": build_security(slot["df"])}
    except ValueError as e:
        raise HTTPException(503, str(e))


@app.get("/api/figures/whale")
def whale_figure() -> dict:
    slot = cache["daily"]["gold_daily_whale_activity"]
    px = cache["daily"]["gold_daily_miner_revenue"]
    if slot["df"] is None or px["df"] is None:
        raise HTTPException(503, "daily cache warming")
    try:
        return {"figure": build_whale(slot["df"], px["df"])}
    except ValueError as e:
        raise HTTPException(503, str(e))


@app.get("/api/figures/footprint")
def footprint_figure() -> dict:
    slot = cache["daily"]["gold_daily_whale_activity"]
    px = cache["daily"]["gold_daily_miner_revenue"]
    if slot["df"] is None or px["df"] is None:
        raise HTTPException(503, "daily cache warming")
    try:
        return {"figure": build_footprint(slot["df"], px["df"])}
    except ValueError as e:
        raise HTTPException(503, str(e))


@app.get("/api/figures/heatmap")
def heatmap_figure() -> dict:
    slot = cache["weekly"]["gold_weekly_fee_heatmap"]
    if slot["df"] is None:
        raise HTTPException(503, "weekly cache warming")
    try:
        return {"figure": build_heatmap(slot["df"])}
    except ValueError as e:
        raise HTTPException(503, str(e))


@app.get("/api/figures/tape")
def tape_figure() -> dict:
    slot = cache["live"]["price_24h"]
    if slot["df"] is None:
        raise HTTPException(503, "warming: price_24h")
    try:
        return {"figure": build_tape(slot["df"])}
    except ValueError as e:
        raise HTTPException(503, str(e))


@app.get("/api/figures/price")
def price_figure() -> dict:
    slot = cache["daily"]["price_daily"]
    if slot["df"] is None:
        raise HTTPException(503, "warming: price_daily")
    try:
        return {"figure": build_price(slot["df"])}
    except ValueError as e:
        raise HTTPException(503, str(e))


@app.get("/partials/headline")
def headline() -> HTMLResponse:
    import polars as pl

    slot = cache["live"]["live_block_feed"]
    if slot["df"] is None:
        raise HTTPException(503, "live cache warming")
    df = slot["df"].filter(pl.col("btc_usd").is_not_null())
    tip = int(slot["df"]["number"].max())
    net = cache.get("tip")
    lag = f"{net - tip} behind" if isinstance(net, int) and net >= tip else "synced"
    try:
        import httpx

        r = httpx.get("https://mempool.space/api/v1/prices", timeout=5)
        r.raise_for_status()
        last_px = float(r.json()["USD"])
    except Exception:
        last_px = float(df["btc_usd"][-1])
    first_px = float(df["btc_usd"][0])
    chg = (last_px / first_px - 1) * 100
    sign = "+" if chg >= 0 else ""
    color = "#13801f" if chg >= 0 else "#c53030"
    since = str(slot["df"]["timestamp"][0])[:16]
    return HTMLResponse(
        f"<div class='headline'>"
        f"<div><small>TIP</small><strong>{tip:,}</strong></div>"
        f"<div><small>NETWORK</small><strong>{net if isinstance(net, int) else '—'}</strong></div>"
        f"<div><small>LAG</small><strong>{lag}</strong></div>"
        f"<div><small>BTC USD</small><strong>${last_px:,.0f}</strong></div>"
        f"<div><small>SINCE {since}</small><strong style='color:{color}'>{sign}{chg:.2f}%</strong></div>"
        f"</div>"
    )


@app.get("/api/health")
@app.get("/health")
def health() -> dict:
    return {"ok": True}

if __name__ == "__main__":
    pass