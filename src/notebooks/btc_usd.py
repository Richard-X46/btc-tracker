import marimo

__generated_with = "0.23.16"
app = marimo.App(width="full")


@app.cell
def _():
    import yfinance as yf

    return (yf,)


@app.cell
def _(yf):
    data = yf.Ticker("BTC-USD").history(period="max", interval="1d")
    return (data,)


@app.cell
def _(data):
    # range of Date in the data
    print(f"Date range: {data.index.min()} to {data.index.max()}")
    # shape of the data
    print(f"Data shape: {data.shape}")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
