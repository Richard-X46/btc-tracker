import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    from pyarrow import fs as pafs
    from pyarrow import dataset as ds
    from databricks.connect import DatabricksSession
    import os
    from dotenv import load_dotenv
    import duckdb
    import pyarrow as pa
    import gc
    import polars as pl
    import pyspark.sql.functions as F

    pl.Config.set_tbl_cols(-1)
    return DatabricksSession, F, ds, duckdb, gc, load_dotenv, mo, os, pafs


@app.cell
def _(load_dotenv):
    load_dotenv()
    return


@app.cell
def _(gc):
    gc.collect()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Configuration
    - Spark serverless
    - arrow tables
    """)
    return


@app.cell
def _(DatabricksSession, os):
    spark = DatabricksSession.builder.remote(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")
        ).serverless(True).getOrCreate()
    return (spark,)


@app.cell
def _(ds, pafs):
    s3 = pafs.S3FileSystem(anonymous=True, region="us-east-2")
    arrow_tx_path = "aws-public-blockchain/v1.0/btc/transactions/"
    arrow_blk_path = "aws-public-blockchain/v1.0/btc/blocks/"
    # scope directly to one recent partition — no fragment walking, no full-dataset listing
    recent_sample = ds.dataset(
        arrow_tx_path + "date=2026-01-01/",
        filesystem=s3,
        format="parquet",
    )
    target_schema = recent_sample.schema

    # now build the full-range dataset using that schema up front
    transactions = ds.dataset(
        arrow_tx_path,
        filesystem=s3,
        partitioning="hive",
        schema=target_schema,
    )

    blocks = ds.dataset(
        arrow_blk_path,
        filesystem=s3,
        partitioning="hive",
        schema=target_schema,
    )
    return blocks, transactions


@app.cell
def _(blocks, duckdb, transactions):
    # 1. Load the HTTP/S3 extension
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # 2. Configure for a public S3 bucket (no credentials needed)
    con.execute("SET s3_region='us-east-1';") # Change to your bucket's region
    con.execute("SET s3_url_style='path';")

    # registering the arrow dataset as a virtual table in DuckDB
    con.register("transactions", transactions)
    con.register("blocks", blocks)
    return (con,)


@app.cell
def _(transactions):
    transactions.schema
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _(con):
    con.sql("SHOW TABLES").show()
    return


@app.cell
def _(con):
    query =f"""
    SELECT *
    FROM transactions
    WHERE date = '2026-01-12'

    limit 10


    """
    _df = con.sql(query).pl()
    _df.show()
    return


@app.cell
def _():
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        --- DUCKDB ------//////////////

        CREATE OR REPLACE VIEW inputs AS
        with inputs as (
            select txid, date, block_number, input_count, output_count,
                unnest(inputs) as inp
                from (select * from  transactions  where date = '2026-08-11'  
                --and block_number = 930_442  
                limit 10) 

        )
        select txid,date,block_number,input_count,output_count,
                inp.*

                from inputs
        """,
        engine=con
    )
    return


@app.cell
def _():
    # # Today's blocks

    # con.sql("""
    # select  block_number 
    # from transactions 
    # where date = current_date
    #     group by block_number
    # """).show(max_width=10000)
    return


@app.cell
def _(con):


    tx_path = "s3://aws-public-blockchain/v1.0/btc/transactions/*/*.parquet"
    blk_path ="s3://aws-public-blockchain/v1.0/btc/blocks/*/*.parquet"
    # 3. Query the Hive path directly using a glob pattern
    # DuckDB automatically extracts the 'date' column from the directory name!
    _query = f"""
    SELECT  *
    FROM read_parquet('{tx_path}', hive_partitioning=True) 
    -- WHERE date = CURRENT_DATE

    ;
    """
    dtx = con.sql(_query)

    rel = dtx.filter("date >= '2026-01-01'")
    count = rel.count("*")  
    sample = rel.limit(10).pl()
    sample
    return (blk_path,)


@app.cell
def _(blk_path, con):
    _q = f"""
    select * from read_parquet('{blk_path}', hive_partitioning=True) 
    where date = '2026-06-05'

    """

    con.sql(_q).pl().show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## SQL on DATABRICKS CATALOGUE
    """)
    return


@app.cell
def _(os, sql):
    db_con = sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )
    cursor = db_con.cursor()
    return


@app.cell
def _():
    # sample data from blocks table -> Polars DataFrame
    _q = """
    SELECT *
    FROM btc_data.blocks
    WHERE date = '2026-01-05'
    """

    # cursor.execute(_q)
    # arrow_tbl = cursor.fetchall_arrow()   # Databricks SQL connector Arrow result
    # blocks_df = pl.from_arrow(arrow_tbl)

    # blocks_df
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Spark Databricks
    """)
    return


@app.cell
def _():
    return


@app.cell
def _(spark):
    # scanning the existing blocks table in the Databricks catalog - workspace.btc_data.blocks

    data = spark.read.table("btc_data.blocks").filter("date = '2010-01-05'").collect()
    _df = spark.createDataFrame(data)
    print(_df.printSchema())
    _df.show(truncate=False)


    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _(spark):
    from pyspark.sql import functions as F

    # Point at the partition ROOT, not the glob — Spark auto-discovers
    # hive-style partition columns from directory structure when you do this
    tx_path_spark = "s3a://aws-public-blockchain/v1.0/btc/transactions/"

    spx = spark.read.parquet(tx_path_spark)  # lazy — no data read yet, just a plan

    # result = df.filter(F.col("date") == F.current_date())  # still lazy

    # # result.show()  # only now does Spark list + read matching partitions
    # result.select("block_number").distinct().show()  # only now does Spark list + read matching partitions
    return F, spx


@app.cell
def _(spx):
    # inspecting the df plan
    spx.explain()
    return


@app.cell
def _(F, spx):
    # agg by block_number total tx for this month
    spx.filter(F.col("date") == F.current_date()).groupBy("block_number").agg(F.count("*").alias("total_tx")).show()
    return


@app.cell
def _(F, spx):
    test = spx.filter(F.col("date") >= "2026-01-01")  # use a real date string, not current_date() yet — see note below
    # total rows
    test.explain(True)
    return (test,)


@app.cell
def _(F, test):
    test.count()  # trigger it once so it's cached in query history, or:
    test.select(F.count("*")).explain(True)
    return


if __name__ == "__main__":
    app.run()
