"""Dashboard unit tests: builders, routes, cache rollover. No warehouse,
no share, no Spark — synthetic frames only. Run: pytest tests/ -q
"""
import asyncio
import datetime as dt
import json
import sys
from pathlib import Path

import polars as pl
import pytest
from fastapi import HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from btc_tracker.dashboard import figures as F
from btc_tracker.dashboard.cache import _slot, cache, now_utc


def _live(n=30, step_min=10):
    base = dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc)
    rows = [
        {
            "number": 1000 + i,
            "timestamp": base + dt.timedelta(minutes=step_min * i),
            "tx_count": 3000,
            "inputs": 8000,
            "outputs": 9000,
            "value_btc": 4000.0,
            "fees_btc": 0.05,
            "ivl_s": float(step_min * 60) if i else None,
            "btc_usd": 77000.0,
        }
        for i in range(n)
    ]
    return pl.DataFrame(rows)


def _daily(n=10):
    base = dt.date(2026, 10, 1)
    return pl.DataFrame(
        [
            {
                "date": (base + dt.timedelta(days=i)).isoformat(),
                "p50_sat_vbyte": 1.0,
                "p90_sat_vbyte": 4.0,
                "p99_sat_vbyte": 12.0,
                "tx_count": 600000,
                "avg_block_fullness_pct": 98.0,
                "block_count": 144,
            }
            for i in range(n)
        ]
    )


def _miner(n=10):
    base = dt.date(2026, 10, 1)
    return pl.DataFrame(
        [
            {
                "date": (base + dt.timedelta(days=i)).isoformat(),
                "fees_btc": 2.5,
                "settled_btc": 700000.0,
                "tx_count": 600000,
                "block_count": 144,
                "subsidy_btc": 450.0,
                "revenue_btc": 452.5,
                "revenue_usd": 35_000_000.0,
                "fee_share_pct": 0.55,
                "btc_usd": 77000.0,
                "avg_fee_usd": 0.32,
                "friction_bps": 0.036,
            }
            for i in range(n)
        ]
    )


def _whale(n=10):
    base = dt.date(2026, 10, 1)
    return pl.DataFrame(
        [
            {
                "date": (base + dt.timedelta(days=i)).isoformat(),
                "whale_txs": 1000,
                "shark_txs": 5000,
                "total_txs": 600000,
                "big_btc": 800000.0,
                "consolidation": 300,
                "dispersal": 500,
                "top_tx_btc": 20000.0,
                "whale_vbytes": 600000,
                "total_vbytes": 140_000_000,
                "transfer": 200,
                "shark_only": 4000,
                "whale_vbyte_pct": 0.004,
            }
            for i in range(n)
        ]
    )


def _heat():
    rows = []
    for w in range(12):
        wk = (dt.date(2026, 8, 3) + dt.timedelta(weeks=w)).isoformat()
        for dow in range(1, 8):
            for hr in range(24):
                rows.append({"wk": wk, "hr": hr, "dow": dow,
                             "med_sat_vb": 1.0, "p90_sat_vb": 3.0, "txs": 20000})
    return pl.DataFrame(rows)


def _minutes(n=300):
    base = dt.datetime(2026, 11, 3, tzinfo=dt.timezone.utc)
    return pl.DataFrame(
        [
            {"ts": base + dt.timedelta(minutes=i), "open": 77000.0, "high": 77100.0,
             "low": 76900.0, "close": 77050.0, "volume": 2.0}
            for i in range(n)
        ]
    )


def _price_daily(n=400):
    base = dt.date(2025, 10, 1)
    return pl.DataFrame(
        [{"date": (base + dt.timedelta(days=i)).isoformat(),
          "close": 70000.0 + i, "volume": 100.0} for i in range(n)]
    )


def test_jsonable_numpy_datetime():
    import numpy as np
    import pandas as pd

    out = F._jsonable({"a": np.array([1, 2]), "b": np.int64(3),
                       "c": pd.Timestamp("2026-01-01"), "d": (np.float64(1.5),)})
    json.dumps(out)
    assert out == {"a": [1, 2], "b": 3, "c": "2026-01-01T00:00:00", "d": [1.5]}


@pytest.mark.parametrize("fn,args", [
    (F.build_pace, (None,)),
    (F.build_last_n, (None,)),
    (F.build_fees, (None,)),
    (F.build_security, (None,)),
    (F.build_whale, (None, None)),
    (F.build_footprint, (None, None)),
    (F.build_heatmap, (None,)),
    (F.build_tape, (None,)),
    (F.build_price, (None,)),
])


def test_builders_empty_slot(fn, args):
    with pytest.raises(ValueError):
        fn(*args)


def test_pace_math():
    fig = F.build_pace(_live())
    json.dumps(fig)
    names = [t["name"] for t in fig["data"]]
    assert names == ["block", "24-block mean", "latest"]
    assert len(fig["data"][0]["x"]) == 29  # seed row has no interval


def test_last_n_shape():
    fig = F.build_last_n(_live(), 10)
    json.dumps(fig)
    assert len(fig["data"][0]["x"]) == 10


def test_fees_excludes_today_partial():
    df = _daily(10)
    fig = F.build_fees(df)
    json.dumps(fig)
    assert len(fig["data"][0]["x"]) <= 10


def test_security_traces():
    fig = F.build_security(_miner())
    json.dumps(fig)
    assert [t["name"] for t in fig["data"]] == ["avg fee / tx", "fee share of revenue", "7-day mean"]


def test_whale_merge_price():
    fig = F.build_whale(_whale(), _miner())
    json.dumps(fig)
    assert len(fig["data"]) == 3


def test_footprint_traces():
    fig = F.build_footprint(_whale(), _miner())
    json.dumps(fig)
    assert [t["name"] for t in fig["data"]] == ["dispersal", "consolidation", "transfer", "BTC USD"]


def test_heatmap_shape():
    fig = F.build_heatmap(_heat())
    json.dumps(fig, allow_nan=False)
    assert (len(fig["data"][0]["z"]), len(fig["data"][0]["z"][0])) == (7, 24)


def test_tape_minutely():
    fig = F.build_tape(_minutes())
    json.dumps(fig)
    assert len(fig["data"]) == 5


def test_price_range():
    fig = F.build_price(_price_daily())
    json.dumps(fig)
    assert len(fig["data"][0]["x"]) == 400


def _fill(tier, name, df):
    cache[tier][name].update(df=df, asof=now_utc(), ok=True, stale=False, error=None)


def test_routes_shape_and_cold():
    import btc_tracker.dashboard.app as app

    _fill("live", "live_block_feed", _live())
    out = app.pace_figure()
    assert set(out) == {"figure"} and "data" in out["figure"]

    cache["live"]["live_block_feed"] = _slot()
    with pytest.raises(HTTPException) as e:
        app.pace_figure()
    assert e.value.status_code == 503


def test_headline_html():
    import btc_tracker.dashboard.app as app

    _fill("live", "live_block_feed", _live())
    cache["tip"] = 1030
    r = app.headline()
    body = r.body.decode()
    assert "1,029" in body and "LAG" in body


def test_daily_rollover_skips_fresh():
    from btc_tracker.dashboard import poller

    async def fail(*a, **k):
        raise AssertionError("should not pull when fresh")

    poller._pull, orig = fail, poller._pull
    try:
        _fill("daily", "gold_daily_fee_metrics", _daily())
        asyncio.run(poller.refresh_daily())
    finally:
        poller._pull = orig


def test_weekly_rollover_pulls_stale():
    from btc_tracker.dashboard import poller

    calls = []

    async def fake(table, sql, hints=None, tries=3):
        calls.append(table)
        return _heat()

    poller._pull, orig = fake, poller._pull
    try:
        cache["weekly"]["gold_weekly_fee_heatmap"] = _slot()
        asyncio.run(poller.refresh_weekly())
        assert calls == ["gold_weekly_fee_heatmap"]
    finally:
        poller._pull = orig
