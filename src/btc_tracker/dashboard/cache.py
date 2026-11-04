"""Dashboard cache: mempool watcher state + gold table slots.

Readers serve these or 503; they never touch the share. A failed refresh
keeps last-good and flags stale — it never empties a slot.
"""

from datetime import datetime, timezone


def _slot():
    return {"df": None, "asof": None, "ok": False, "stale": False, "error": None}


def now_utc():
    return datetime.now(timezone.utc)


cache = {
    "blocks": [],
    "mempool": [],
    "tip": None,
    # live: refreshed on job completion (event-driven)
    "live": {
        "live_block_feed": _slot(),
        "price_24h": _slot(),
    },
    # daily: refreshed once per UTC date (price_daily is app-fetched, no table)
    "daily": {
        "gold_daily_fee_metrics": _slot(),
        "gold_daily_miner_revenue": _slot(),
        "gold_daily_whale_activity": _slot(),
        "price_daily": _slot(),
    },
    # weekly: refreshed when max wk goes stale
    "weekly": {
        "gold_weekly_fee_heatmap": _slot(),
    },
}
