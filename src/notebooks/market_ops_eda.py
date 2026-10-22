import marimo

__generated_with = "0.23.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    return


@app.cell
def _():
    import yfinance as yf

    ibit = yf.Ticker("IBIT")

    return (ibit,)


@app.cell
def _(ibit):
    # strike price
    ibit.option_chain()[1]

    return


@app.cell
def _(ibit):
    exp = '2026-05-13'
    chain = ibit.option_chain(exp)
    calls = chain.calls

    spot = 45.80
    atm = calls[calls['strike'] == 45.0].iloc[0]

    print(f"Strike:     ${atm['strike']}")
    print(f"Premium:    ${atm['lastPrice']}")
    print(f"Bid/Ask:    ${atm['bid']} / ${atm['ask']}")
    print(f"IV:         {atm['impliedVolatility']:.1%}")
    print(f"Volume:     {atm['volume']:.0f}")
    print(f"OI:         {atm['openInterest']:.0f}")

    # expected move
    import numpy as np
    expected_move = spot * atm['impliedVolatility'] * np.sqrt(1/365)
    print(f"\nExpected move tomorrow: ±${expected_move:.2f}")
    print(f"Range: ${spot-expected_move:.2f} to ${spot+expected_move:.2f}")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
