import marimo

__generated_with = "0.23.16"
app = marimo.App(width="full")


@app.cell
def _():
    import polars as pl
    import os
    import subprocess
    pl.Config.set_tbl_cols(-1)  # Show all columns in the output
    return pl, subprocess


@app.cell
def _():
    return


@app.cell
def _(subprocess):
    print(subprocess.run(["tree","../.."],capture_output=True,text= True).stdout)

    return


@app.cell
def _(subprocess):
    print(subprocess.run(["ls","../../src/data/sample"],capture_output=True,text= True).stdout)
    return


@app.cell
def _():
    tx_sample_path = "../../src/data/sample/transaction.parquet"
    blk_sample_path = "../../src/data/sample/blocks.parquet"
    return blk_sample_path, tx_sample_path


@app.cell
def _(pl, tx_sample_path):
    df = pl.scan_parquet(tx_sample_path).limit(5).collect()
    df
    return


@app.cell
def _(blk_sample_path, pl):
    blk = pl.scan_parquet(blk_sample_path).limit(5).collect()
    blk
    return (blk,)


@app.cell
def _(blk):
    for column, dtype in blk.schema.items():
        print(f"{column}: {dtype}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Output is truncated. View as a scrollable element or open in a text editor. Adjust cell output settings...
    root
     |-- hash: string (nullable = true)
     |-- version: long (nullable = true)
     |-- mediantime: timestamp (nullable = true)
     |-- nonce: long (nullable = true)
     |-- bits: string (nullable = true)
     |-- difficulty: double (nullable = true)
     |-- chainwork: string (nullable = true)
     |-- size: long (nullable = true)
     |-- weight: long (nullable = true)
     |-- coinbase_param: string (nullable = true)
     |-- number: long (nullable = true)
     |-- transaction_count: long (nullable = true)
     |-- merkle_root: string (nullable = true)
     |-- stripped_size: long (nullable = true)
     |-- timestamp: timestamp (nullable = true)
     |-- date: date (nullable = true)
     |-- last_modified: timestamp (nullable = true)
    """)
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
