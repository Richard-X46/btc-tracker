import marimo

__generated_with = "0.23.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import pandas as pd

    return mo, pd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Historical implied volatility (DVOL index) — no key needed
    GET https://www.deribit.com/api/v2/public/get_historical_volatility?currency=BTC

    ### Live options chain
    GET https://www.deribit.com/api/v2/public/get_instruments?currency=BTC&kind=option&expired=false

    ### Order book for a specific contract (gives mark IV, greeks)
    GET https://www.deribit.com/api/v2/public/get_order_book?instrument_name=BTC-27JUN25-100000-C
    """)
    return


@app.cell
def _():
    import requests as r

    url = "https://www.deribit.com/api/v2/public/get_historical_volatility?currency=BTC"
    response = r.get(url)
    data = response.json()
    print(data)
    import requests

    url = "https://www.deribit.com/api/v2/public/get_historical_volatility"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    response = requests.get(url, params={"currency": "BTC"}, headers=headers)
    print(response.json())
    return (response,)


@app.cell
def _(response):
    res_data = response.json()
    return (res_data,)


@app.cell
def _(pd, res_data):
    df = pd.DataFrame(res_data["result"], columns=["timestamp", "volatility"])
    return (df,)


@app.cell
def _(df, pd):
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

    return


@app.cell
def _(df):
    # range of the data
    print(df['timestamp'].min(), df['timestamp'].max())
    return


@app.cell
def _(df):
    # group by date and take count of records per day
    df['date'] = df['timestamp'].dt.date
    daily_counts = df.groupby('date').size()
    print(daily_counts)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
