"""Server-side figure builders: cache frame in, Plotly JSON out.

Browser renders the pixels (plotCard + Plotly.newPlot); these functions only
compute traces/layout/hover. One builder per card; pace first.
"""
import math

import plotly.graph_objects as go

ROLL = 24


def _jsonable(o):
    """to_plotly_json leaves numpy/pandas/datetime scalars behind; coerce."""
    import datetime as _dt
    import numpy as np
    import pandas as pd
    if isinstance(o, np.ndarray):
        return [_jsonable(v) for v in o.tolist()]
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, (np.floating, float)):
        return float(o)
    if isinstance(o, (pd.Timestamp, _dt.datetime, _dt.date)):
        return o.isoformat()
    if isinstance(o, dict):
        return {k: _jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_jsonable(v) for v in o]
    return o


def build_pace(df, n: int = 120):
    """Block-interval dots + 24-block mean from live_block_feed.

    Expects columns: number, timestamp, tx_count, ivl_s. Raises ValueError
    when the slot is empty (route converts to 503).
    """
    if df is None or df.height == 0:
        raise ValueError("live_block_feed cache empty")
    p = df.tail(n).to_pandas()
    p["timestamp"] = __import__("pandas").to_datetime(p["timestamp"])
    p = p.sort_values("number").reset_index(drop=True)
    p["ivl_min"] = p["ivl_s"] / 60.0
    p = p[p["ivl_min"].notna() & (p["ivl_min"] > 0)].reset_index(drop=True)
    if p.empty:
        raise ValueError("no positive intervals yet")
    p["roll"] = p["ivl_min"].rolling(ROLL, min_periods=6).mean()

    n0 = int(p["number"].iloc[0])
    elapsed_min = (p["timestamp"] - p["timestamp"].iloc[0]).dt.total_seconds() / 60.0
    p["lag_blocks"] = elapsed_min / 10.0 - (p["number"] - n0)
    last = p.iloc[-1]

    tmin, tmax = float(p["tx_count"].min()), float(p["tx_count"].max())
    p["msize"] = 6.0 + 16.0 * (p["tx_count"] - tmin) / (tmax - tmin) if tmax > tmin else 10.0

    DIV = [[0.0, "#009E73"], [0.5, "#F0E442"], [1.0, "#D55E00"]]
    cmax = float(max(20.0, p["ivl_min"].quantile(0.99)))
    q10 = -10 * math.log(0.90)
    q90 = -10 * math.log(0.10)
    ymax = max(25.0, float(p["ivl_min"].quantile(0.99)) * 1.15)

    fig = go.Figure()
    fig.add_hrect(y0=q10, y1=q90, fillcolor="rgba(0,158,115,0.07)",
                  line_width=0, layer="below")
    fig.add_hline(y=10, line_dash="dash", line_color="black",
                  annotation_text="10-min target", annotation_position="bottom right")
    x_last = last["timestamp"].to_pydatetime()
    fig.add_shape(type="line", xref="x", yref="y domain", x0=x_last, x1=x_last,
                  y0=0, y1=1, line=dict(color="black", dash="dot", width=1))

    fig.add_trace(go.Scatter(
        x=p["timestamp"], y=p["ivl_min"], mode="markers", name="block",
        marker=dict(size=p["msize"], color=p["ivl_min"], colorscale=DIV,
                    cmin=0.0, cmid=10.0, cmax=cmax, showscale=True,
                    colorbar=dict(title="min", thickness=10, len=0.55, y=0.72),
                    line=dict(color="black", width=0.4)),
        customdata=list(zip(p["number"].astype(int), p["tx_count"], p["lag_blocks"])),
        hovertemplate=("block %{customdata[0]}<br>%{x}<br>%{y:.1f} min<br>"
                       "%{customdata[1]:,.0f} txs<br>lag %{customdata[2]:+.0f} blocks"
                       "<extra></extra>")))
    fig.add_trace(go.Scatter(x=p["timestamp"], y=p["roll"], mode="lines",
        name=f"{ROLL}-block mean", line=dict(color="#E69F00", width=2.2)))
    fig.add_trace(go.Scatter(x=[last["timestamp"]], y=[last["ivl_min"]], mode="markers",
        name="latest", marker=dict(symbol="circle-open", size=22, color="black",
                                   line=dict(width=2)), hoverinfo="skip",
        showlegend=False))

    fig.add_annotation(x=1, y=0, xref="x domain", yref="y domain", xanchor="right",
        yanchor="bottom", showarrow=False,
        text=(f"median {p['ivl_min'].median():.1f} min  |  "
              f"{100 * (p['ivl_min'] < 10).mean():.0f}% under 10 min  |  "
              f"{ROLL}-block mean {float(last['roll']):.1f} min"),
        font=dict(size=11))
    fig.update_layout(height=450, hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    font=dict(size=10)),
        margin=dict(t=20, b=40, l=60, r=20),
        xaxis_title="block time", yaxis_title="minutes since previous block",
        yaxis=dict(range=[0, ymax]))
    fig.update_xaxes(showline=True, linecolor="black", mirror=True)
    fig.update_yaxes(showline=True, linecolor="black", mirror=True)
    return _jsonable(fig.to_plotly_json())


def build_fees(df):
    """Fee market: p50/p90/p99 sat/vbyte lines (Graph 1-1).

    Partial today excluded (intraday percentiles mislead). Raises ValueError
    when the slot is empty (route converts to 503).
    """
    import datetime as _dt
    import pandas as pd

    if df is None or df.height == 0:
        raise ValueError("gold_daily_fee_metrics cache empty")
    g = df.to_pandas()
    g["date"] = pd.to_datetime(g["date"])
    today = pd.Timestamp(_dt.datetime.now(_dt.timezone.utc).date())
    g = g[g["date"] < today].sort_values("date").reset_index(drop=True)
    if g.empty:
        raise ValueError("no finalized fee days yet")

    fig = go.Figure()
    for col in ["p50_sat_vbyte", "p90_sat_vbyte", "p99_sat_vbyte"]:
        fig.add_trace(go.Scatter(x=g["date"], y=g[col], name=col))
    fig.update_layout(showlegend=True, height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    font=dict(size=10)),
        margin=dict(t=20, b=40, l=60, r=20),
        xaxis_title="date", yaxis_title="sat/vbyte", hovermode="x unified",
        xaxis=dict(showline=True, linecolor="black", mirror=True,
            rangeselector=dict(buttons=[
                dict(count=3, label="3m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(step="all")])),
        yaxis=dict(showline=True, linecolor="black", mirror=True))
    return _jsonable(fig.to_plotly_json())


def build_last_n(df, n: int = 10):
    """Last-N blocks: BTC settled bars + BTC price line (Graph 2).

    Simple fixed window (default 10). Expects live_block_feed columns.
    Raises ValueError when the slot is empty (route converts to 503).
    """
    import pandas as pd

    if df is None or df.height == 0:
        raise ValueError("live_block_feed cache empty")
    a = df.tail(n).to_pandas()
    a["timestamp"] = pd.to_datetime(a["timestamp"])
    a = a.sort_values("number").reset_index(drop=True)
    for col in ("tx_count", "inputs", "outputs", "value_btc", "fees_btc", "btc_usd"):
        a[col] = a[col].astype(float)
    a["btc_usd"] = a["btc_usd"].ffill().bfill()
    if a["btc_usd"].isna().all():
        a["btc_usd"] = 0.0
    a["label"] = a["timestamp"].dt.strftime("%H:%M")
    xs = a["number"].astype(int).tolist()

    def fmt(v):
        return f"{v / 1000:.1f}k" if v >= 1000 else f"{v:.0f}"

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=a["label"], y=a["value_btc"], name="BTC settled",
        marker_color="#F7931A", marker_line_color="black", marker_line_width=0.6,
        customdata=xs,
        hovertemplate="block %{customdata}<br>%{y:,.0f} BTC settled<extra></extra>",
        text=[f"{fmt(t)} txs<br>{fmt(i)} in -> {fmt(o)}"
              for t, i, o in zip(a["tx_count"], a["inputs"], a["outputs"])],
        textposition="outside", textfont=dict(size=10, color="black")))
    fig.add_trace(go.Bar(
        x=a["label"], y=a["value_btc"], name="block",
        showlegend=False,
        marker_color="rgba(0,0,0,0)", marker_line_width=0,
        text=[f"{n}" for n in xs],
        textposition="inside", insidetextanchor="middle",
        textfont=dict(size=11, color="#14532D"),
        hoverinfo="skip"))
    fig.add_trace(go.Scatter(
        x=a["label"], y=a["btc_usd"], name="BTC USD", yaxis="y2", mode="lines+markers",
        line=dict(color="black", width=2), marker=dict(size=6),
        hovertemplate="$%{y:,.0f}<extra></extra>"))

    avg = float(a["value_btc"].mean())
    fig.add_hline(y=avg, line=dict(color="gray", dash="dot", width=1),
                  annotation_text=f"avg {avg:,.0f} BTC", annotation_position="top left")
    fig.update_layout(height=440, hovermode="x unified", barmode="overlay",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    font=dict(size=10)),
        margin=dict(t=30, b=40, l=70, r=60),
        xaxis_title="block time (UTC)", yaxis_title="BTC settled per block",
        yaxis=dict(range=[0, float(a["value_btc"].max()) * 1.55]),
        yaxis2=dict(title="BTC USD", overlaying="y", side="right", showgrid=False))
    fig.update_xaxes(showline=True, linecolor="black", mirror=True)
    fig.update_yaxes(showline=True, linecolor="black", mirror=True)
    return _jsonable(fig.to_plotly_json())


def build_security(df):
    """Security squeeze: fee share of revenue + avg fee per tx (Graph 3-1).

    Partial today excluded. Raises ValueError when the slot is empty
    (route converts to 503).
    """
    import datetime as _dt
    import pandas as pd

    if df is None or df.height == 0:
        raise ValueError("gold_daily_miner_revenue cache empty")
    r = df.to_pandas()
    r["date"] = pd.to_datetime(r["date"])
    today = pd.Timestamp(_dt.datetime.now(_dt.timezone.utc).date())
    r = r[r["date"] < today].sort_values("date").reset_index(drop=True)
    if r.empty:
        raise ValueError("no finalized revenue days yet")
    r["share_7d"] = r["fee_share_pct"].rolling(7, min_periods=3).mean()

    fig = go.Figure()
    fig.add_trace(go.Bar(x=r["date"], y=r["avg_fee_usd"], name="avg fee / tx",
        marker_color="rgba(115,115,115,0.35)", marker_line_color="#737373",
        marker_line_width=0.6,
        hovertemplate="%{x|%b %d}<br>$%{y:.2f} / tx<extra></extra>"))
    fig.add_trace(go.Scatter(x=r["date"], y=r["fee_share_pct"], mode="lines+markers",
        name="fee share of revenue", yaxis="y2",
        line=dict(color="#D55E00", width=2), marker=dict(size=4),
        hovertemplate="%{x|%b %d}<br>%{y:.2f}% of miner revenue<extra></extra>"))
    fig.add_trace(go.Scatter(x=r["date"], y=r["share_7d"], mode="lines",
        name="7-day mean", yaxis="y2",
        line=dict(color="#E69F00", width=1.5, dash="dash"), hoverinfo="skip"))

    last = r.iloc[-1]
    fig.add_annotation(x=1, y=0, xref="x domain", yref="y domain", xanchor="right",
        yanchor="bottom", showarrow=False,
        text=(f"fee share {last['fee_share_pct']:.2f}% (7d {last['share_7d']:.2f}%)  |  "
              f"avg fee ${last['avg_fee_usd']:.2f}/tx  |  friction {last['friction_bps']:.2f} bps"),
        font=dict(size=11))
    fig.update_layout(showlegend=True, height=440,
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1, font=dict(size=10)),
        margin=dict(t=30, b=60, l=60, r=60),
        xaxis_title="date", hovermode="x unified",
        xaxis=dict(showline=True, linecolor="black", mirror=True,
            rangeselector=dict(buttons=[
                dict(count=3, label="3m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(step="all")])),
        yaxis=dict(showline=True, linecolor="black", mirror=True,
                   title="avg fee USD / tx", tickformat="$,.2f"),
        yaxis2=dict(showline=True, linecolor="black", mirror=True,
                    title="fee share of revenue %", overlaying="y", side="right",
                    showgrid=False, ticksuffix="%"))
    return _jsonable(fig.to_plotly_json())


def build_whale(df, px):
    """Shark/whale tx counts + BTC USD (Graph 2-1).

    Price comes from the miner slot (whale model is BTC-native).
    Partial today excluded. Raises ValueError when a slot is empty
    (route converts to 503).
    """
    import datetime as _dt
    import pandas as pd

    if df is None or df.height == 0:
        raise ValueError("gold_daily_whale_activity cache empty")
    if px is None or px.height == 0:
        raise ValueError("gold_daily_miner_revenue cache empty (price)")
    g = df.to_pandas()
    g["date"] = pd.to_datetime(g["date"])
    today = pd.Timestamp(_dt.datetime.now(_dt.timezone.utc).date())
    g = g[g["date"] < today].reset_index(drop=True)
    p = px.to_pandas()[["date", "btc_usd"]].copy()
    p["date"] = pd.to_datetime(p["date"])
    g = g.merge(p, on="date", how="left").sort_values("date").reset_index(drop=True)
    if g.empty:
        raise ValueError("no finalized whale days yet")
    g["shark_only"] = g["shark_txs"] - g["whale_txs"]
    x1 = g["date"].iloc[-1]
    x0 = x1 - pd.Timedelta(days=93)

    fig = go.Figure()
    fig.add_trace(go.Bar(x=g["date"], y=g["shark_only"], name="shark (10-100 BTC)",
        marker_color="rgba(230,159,0,0.35)", marker_line_color="#E69F00", marker_line_width=1.5))
    fig.add_trace(go.Bar(x=g["date"], y=g["whale_txs"], name="whale (>100 BTC)",
        marker_color="rgba(86,180,233,0.35)", marker_line_color="#56B4E9", marker_line_width=1.5))
    fig.add_trace(go.Scatter(x=g["date"], y=g["btc_usd"], name="BTC USD", yaxis="y2",
        line=dict(color="black", width=1.5)))
    fig.update_layout(
        barmode="stack",
        showlegend=True, height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1, font=dict(size=10)),
        margin=dict(t=20, b=40, l=60, r=60),
        xaxis_title="date", yaxis_title="txs / day",
        hovermode="x unified",
        xaxis=dict(
            range=[x0, x1],
            showline=True, linecolor="black", mirror=True,
            rangeselector=dict(buttons=[
                dict(count=3, label="3m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(step="all")])),
        yaxis=dict(showline=True, linecolor="black", mirror=True, title="txs / day"),
        yaxis2=dict(overlaying="y", side="right", showgrid=False,
                    showline=True, linecolor="black", title="BTC USD"))
    return _jsonable(fig.to_plotly_json())


def build_footprint(df, px):
    """Whale footprint: dispersal vs consolidation vs transfer + price (Graph 2-4).

    Shape columns come from the model; price from the miner slot.
    Partial today excluded. Raises ValueError when a slot is empty
    (route converts to 503).
    """
    import datetime as _dt
    import pandas as pd

    if df is None or df.height == 0:
        raise ValueError("gold_daily_whale_activity cache empty")
    if px is None or px.height == 0:
        raise ValueError("gold_daily_miner_revenue cache empty (price)")
    g = df.to_pandas()
    g["date"] = pd.to_datetime(g["date"])
    today = pd.Timestamp(_dt.datetime.now(_dt.timezone.utc).date())
    g = g[g["date"] < today].reset_index(drop=True)
    p = px.to_pandas()[["date", "btc_usd"]].copy()
    p["date"] = pd.to_datetime(p["date"])
    g = g.merge(p, on="date", how="left").sort_values("date").reset_index(drop=True)
    if g.empty:
        raise ValueError("no finalized whale days yet")
    x1 = g["date"].iloc[-1]
    x0 = x1 - pd.Timedelta(days=93)

    fig = go.Figure()
    for col, name, fill, line in [("dispersal", "dispersal", "rgba(0,158,115,0.55)", "#009E73"),
                                  ("consolidation", "consolidation", "rgba(230,159,0,0.55)", "#E69F00"),
                                  ("transfer", "transfer", "rgba(204,121,167,0.35)", "#CC79A7")]:
        fig.add_trace(go.Bar(x=g["date"], y=g[col], name=name,
            marker_color=fill, marker_line_color=line, marker_line_width=1))
    fig.add_trace(go.Scatter(x=g["date"], y=g["btc_usd"], name="BTC USD", yaxis="y2",
        line=dict(color="black", width=1.5)))
    fig.update_layout(
        barmode="stack",
        showlegend=True, height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1, font=dict(size=10)),
        margin=dict(t=20, b=40, l=60, r=60),
        xaxis_title="date", hovermode="x unified",
        xaxis=dict(
            range=[x0, x1],
            showline=True, linecolor="black", mirror=True,
            rangeselector=dict(buttons=[
                dict(count=3, label="3m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(step="all")])),
        yaxis=dict(showline=True, linecolor="black", mirror=True, title="whale txs / day"),
        yaxis2=dict(overlaying="y", side="right", showgrid=False,
                    showline=True, linecolor="black", title="BTC USD"))
    return _jsonable(fig.to_plotly_json())


def build_heatmap(df):
    """Cheapest-hours heatmap (Graph 4-1): hour x weekday median sat/vb.

    4w/8w/12w buttons swap the z matrix. Raises ValueError when the slot
    is empty (route converts to 503).
    """
    import pandas as pd

    if df is None or df.height == 0:
        raise ValueError("gold_weekly_fee_heatmap cache empty")
    tod = df.to_pandas()
    tod["wk"] = pd.to_datetime(tod["wk"])
    maxwk = tod["wk"].max()
    days = ["", "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    wins = {"4w": 28, "8w": 56, "12w": 84}
    pivs = {}
    for name, span in wins.items():
        s = tod[tod["wk"] >= maxwk - pd.Timedelta(days=span)]
        pivs[name] = s.pivot_table(index="dow", columns="hr", values="med_sat_vb",
                                   aggfunc="median").reindex(range(1, 8))
    b = tod.loc[tod["med_sat_vb"].idxmin()]
    cheapest = f"cheapest recently: {days[int(b['dow'])]} {int(b['hr']):02d}:00 UTC ({b['med_sat_vb']:.2f} sat/vb)"

    fig = go.Figure(go.Heatmap(z=pivs["4w"].values.tolist(), x=list(range(24)),
        y=[days[d] for d in pivs["4w"].index], colorscale="YlOrRd",
        xgap=1, ygap=1,
        hovertemplate="%{y} %{x}:00 UTC<br>%{z:.2f} sat/vb"))
    fig.add_annotation(x=1, y=0, xref="x domain", yref="y domain", xanchor="right",
        yanchor="bottom", showarrow=False, text=cheapest, font=dict(size=11))
    fig.update_layout(height=300,
        plot_bgcolor="black",
        margin=dict(t=20, b=40, l=60, r=20),
        xaxis_title="hour (UTC)", yaxis_title="",
        xaxis=dict(showline=True, linecolor="black", mirror=True),
        yaxis=dict(showline=True, linecolor="black", mirror=True),
        updatemenus=[dict(type="buttons", direction="right", x=0, y=1.12,
            showactive=True, buttons=[
                dict(method="restyle", label=n,
                     args=[{"z": [pivs[n].values.tolist()]}]) for n in ("4w", "8w", "12w")])])
    return _jsonable(fig.to_plotly_json())
def build_tape(df):
    """Intraday tape, minutely only: 5m candles + session VWAP + EMA 9/21.

    Raises ValueError when the slot is empty (route converts to 503).
    """
    import pandas as pd
    from plotly.subplots import make_subplots

    if df is None or df.height == 0:
        raise ValueError("price_24h cache empty")
    m = df.to_pandas()
    for col in ("open", "high", "low", "close", "volume"):
        m[col] = m[col].astype(float)
    m["ts"] = pd.to_datetime(m["ts"])
    m = m.sort_values("ts").reset_index(drop=True)
    m = m[m["ts"] >= m["ts"].max() - pd.Timedelta(hours=24)]
    m = (m.set_index("ts")
          .resample("5min")
          .agg({"open": "first", "high": "max", "low": "min",
                "close": "last", "volume": "sum"})
          .dropna().reset_index())
    m["sess"] = m["ts"].dt.date
    m["typ"] = (m["high"] + m["low"] + m["close"]) / 3.0
    m["cumvol"] = m.groupby("sess")["volume"].cumsum()
    m["vwap"] = (m["typ"] * m["volume"]).groupby(m["sess"]).cumsum() / m["cumvol"].replace(0, float("nan"))
    m["ema9"] = m["close"].ewm(span=9, adjust=False).mean()
    m["ema21"] = m["close"].ewm(span=21, adjust=False).mean()
    m["up"] = m["close"] >= m["open"]
    mret = (m["close"].iloc[-1] / m["close"].iloc[0] - 1) * 100
    mrng = m["close"].max() - m["close"].min()

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.72, 0.28], vertical_spacing=0.05)
    fig.add_trace(go.Candlestick(
        x=m["ts"], open=m["open"], high=m["high"], low=m["low"], close=m["close"],
        name="BTC USD", increasing_line_color="#26a69a", increasing_fillcolor="#26a69a",
        decreasing_line_color="#ef5350", decreasing_fillcolor="#ef5350",
        whiskerwidth=0.6), row=1, col=1)
    fig.add_trace(go.Scatter(x=m["ts"], y=m["vwap"], mode="lines", name="VWAP",
        line=dict(color="#E69F00", dash="dash", width=1.5)), row=1, col=1)
    fig.add_trace(go.Scatter(x=m["ts"], y=m["ema9"], mode="lines", name="EMA 9",
        line=dict(color="#0072B2", width=1.2)), row=1, col=1)
    fig.add_trace(go.Scatter(x=m["ts"], y=m["ema21"], mode="lines", name="EMA 21",
        line=dict(color="#D55E00", width=1.2)), row=1, col=1)
    fig.add_trace(go.Bar(x=m["ts"], y=m["volume"], name="volume", showlegend=False,
        marker_color=["rgba(200,200,200,0.6)" if u else "black" for u in m["up"]],
        marker_line_width=0), row=2, col=1)
    fig.add_annotation(x=1, y=0, xref="x domain", yref="y domain", xanchor="right",
        yanchor="bottom", showarrow=False,
        text=f"24h return {mret:+.2f}%  |  range ${mrng:,.0f}  |  last ${m['close'].iloc[-1]:,.0f}",
        font=dict(size=11))
    fig.update_layout(height=440, hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    font=dict(size=10)),
        margin=dict(t=20, b=40, l=60, r=20),
        xaxis_rangeslider_visible=False)
    fig.update_xaxes(showticklabels=False)
    fig.update_xaxes(showticklabels=True, row=2, col=1)
    fig.update_xaxes(showline=True, linecolor="black", mirror=True)
    fig.update_yaxes(showline=True, linecolor="black", mirror=True)
    fig.update_yaxes(title_text="BTC USD", row=1, col=1)
    fig.update_yaxes(title_text="vol", row=2, col=1)
    return _jsonable(fig.to_plotly_json())


def build_price(df):
    """Daily close + EMA 9/21 + volume, long range. App-fetched Coinbase daily.

    Raises ValueError when the slot is empty (route converts to 503).
    """
    import pandas as pd

    if df is None or df.height == 0:
        raise ValueError("price cache empty")
    t = df.to_pandas()
    t["date"] = pd.to_datetime(t["date"])
    t = t.sort_values("date").reset_index(drop=True)
    t["ema9"] = t["close"].ewm(span=9, adjust=False).mean()
    t["ema21"] = t["close"].ewm(span=21, adjust=False).mean()
    t["up"] = t["close"] >= t["close"].shift(1).fillna(t["close"])

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=t["date"], y=t["close"], name="close",
        line=dict(color="black", width=1.8),
        hovertemplate="%{x|%b %d %Y}<br>$%{y:,.0f}<extra></extra>"))
    fig.add_trace(go.Scatter(x=t["date"], y=t["ema9"], mode="lines", name="EMA 9",
        line=dict(color="#0072B2", width=1.2)))
    fig.add_trace(go.Scatter(x=t["date"], y=t["ema21"], mode="lines", name="EMA 21",
        line=dict(color="#D55E00", width=1.2)))
    fig.add_trace(go.Bar(x=t["date"], y=t["volume"], name="volume", yaxis="y2",
        marker_color="rgba(115,115,115,0.35)",
        hovertemplate="%{x|%b %d %Y}<br>%{y:,.0f}<extra></extra>"))
    last = t.iloc[-1]
    fig.add_annotation(x=1, y=0, xref="x domain", yref="y domain", xanchor="right",
        yanchor="bottom", showarrow=False,
        text=f"last ${last['close']:,.0f}",
        font=dict(size=11))

    def _win(days):
        w = t[t["date"] >= t["date"].max() - pd.Timedelta(days=days)] if days else t
        lo, hi = float(w["close"].min()), float(w["close"].max())
        pad = (hi - lo) * 0.08 or 1.0
        return ([w["date"].min(), w["date"].max()], [lo - pad, hi + pad])

    _x6m, _y6m = _win(183)
    _x1y, _y1y = _win(365)
    _xall, _yall = _win(0)
    fig.update_layout(showlegend=True, height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    font=dict(size=10)),
        margin=dict(t=20, b=40, l=60, r=60),
        xaxis_title="date", hovermode="x unified",
        xaxis=dict(range=[_x6m[0], _x6m[1]], showline=True, linecolor="black", mirror=True),
        yaxis=dict(range=_y6m, showline=True, linecolor="black", mirror=True,
                   title="BTC USD", tickformat="$,.0f"),
        yaxis2=dict(overlaying="y", side="right", showgrid=False,
                    showline=True, linecolor="black", title="volume"),
        updatemenus=[dict(type="buttons", direction="right", x=0, y=1.12,
            showactive=True, buttons=[
                dict(label="6m", method="relayout",
                     args=[{"xaxis.range": _x6m, "yaxis.range": _y6m}]),
                dict(label="1y", method="relayout",
                     args=[{"xaxis.range": _x1y, "yaxis.range": _y1y}]),
                dict(label="all", method="relayout",
                     args=[{"xaxis.range": _xall, "yaxis.range": _yall}])])])
    return _jsonable(fig.to_plotly_json())
