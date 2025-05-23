from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import plotly.graph_objects as go
import plotly.io as pio
from fastapi import Request
import pandas as pd
pio.templates.default = "plotly_white"
from fastapi.middleware.cors import CORSMiddleware


# get btc price last 10 years from yfinance

def get_btc_price(period: str) -> pd.DataFrame:
    import yfinance as yf
    btc = yf.Ticker("BTC-USD")
    btc_price = btc.history(period=period)
    btc_price.reset_index(inplace=True)
    return btc_price

def get_eth_price(period: str) -> pd.DataFrame:
    import yfinance as yf
    eth = yf.Ticker("ETH-USD")
    eth_price = eth.history(period=period)
    eth_price.reset_index(inplace=True)
    return eth_price




app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],   
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def btc_eth_plot(request: Request):
    period = request.query_params.get("period", "10y")
    btc_price = get_btc_price(period)  # Get full data once
    eth_price = get_eth_price(period)  # Get full data once


    period_map = {
        "10y": pd.DateOffset(years=10),
        "5y": pd.DateOffset(years=5),
        "1y": pd.DateOffset(years=1),
        "6mo": pd.DateOffset(months=6),
        "3mo": pd.DateOffset(months=3),
    }
    now = btc_price.index.max()
    if period in period_map:
        start_date = now - period_map[period]
        btc_price = btc_price[btc_price.index >= start_date]
        eth_price = eth_price[eth_price.index >= start_date]

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=btc_price.index, y=btc_price['Close'], mode='lines', name='BTC Price'))
    fig.add_trace(go.Scatter(x=eth_price.index, y=eth_price['Close'], mode='lines', name='ETH Price'))
    fig.update_layout(
        title='BTC and ETH Price Over Time',
        xaxis_title='Date',
        yaxis_title='Price (USD)',
        font=dict(family="Arial, Helvetica, sans-serif", size=14)
    )
    plot_html = pio.to_html(fig, full_html=False)
    # Add period selection boxes
    periods = [("10y", "All"), ("5y", "5Y"), ("1y", "1Y"), ("6mo", "6M"), ("3mo", "3M")]
    buttons_html = "".join(
        f'<button onclick="window.location.href=\'/?period={p[0]}\'" {"style=\'font-weight:bold\'" if period==p[0] else ""}>{p[1]}</button> '
        for p in periods
    )
    html = f"""
    <html>
    <head>
        <title>BTC and ETH Price Over Time</title>
    </head>
    <body>
        <div style="margin-bottom:20px;">
            {buttons_html}
        </div>
        {plot_html}
    </body>
    </html>
    """
    return html

