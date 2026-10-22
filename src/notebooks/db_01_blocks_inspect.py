import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")


@app.cell
def _():
    from databricks.connect import DatabricksSession
    import os
    from dotenv import load_dotenv
    import gc
    import marimo as mo
    from datetime import datetime, timedelta
    from pyspark.sql.functions import col
    import polars as pl

    load_dotenv()
    gc.collect()
    return DatabricksSession, col, datetime, mo, os, pl, timedelta


@app.cell
def _(DatabricksSession, os):
    spark = DatabricksSession.builder.remote(
                host=os.getenv("DATABRICKS_HOST"),
                token=os.getenv("DATABRICKS_TOKEN")
            ).serverless(True).getOrCreate()

    # Configs
    TABLE_NAME = "workspace.btc_data.blocks"

    # paths
    tx = 's3://aws-public-blockchain/v1.0/btc/transactions/'
    #dates
    current_date = spark.sql("select current_date()").collect()[0][0]
    return TABLE_NAME, current_date, spark


@app.cell
def _(spark):
    # masking permissions to service principle for btc

    spark.sql("""

    GRANT USE CATALOG ON CATALOG workspace TO `89c2bab4-6f5e-4cd1-9330-66952c9359f7`;

    """)


    spark.sql("""GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA workspace.btc_data TO `89c2bab4-6f5e-4cd1-9330-66952c9359f7`;
    """)
    return


@app.cell
def _():

    # checking block tip on the blocks_source external table

    block_source = "workspace.btc_data.blocks_source"
    tx_source = "workspace.btc_data.tx_source"
    return (block_source,)


@app.cell
def _():
    return


@app.cell
def _(spark):
    # size of the tables
    size_df =spark.sql("""  
    describe detail
    workspace.btc_data.blocks

    """)


    size_df.select("sizeInBytes").collect()[0][0] * 1e-6
    return


@app.cell
def _(F, spark):

    spark.range(10).select(F.current_date().alias("today")).show()
    return


@app.cell
def _(F, block_source, spark):

    rel = spark.table(block_source)

    rel.filter(F.col("date") >= F.lit('2026-08-24')).agg({"number":"max"}).show()
    return


@app.cell
def _(spark):
    def analyze_table(table_name: str):
        query = f"""
        ANALYZE TABLE {table_name} COMPUTE STORAGE METRICS
        """
        spark.sql(query).show(truncate=False)

    # analyze_table(TABLE_NAME)
    return


@app.cell
def _():
    # bytes to mb
    val = 199664774
    mb = val / (1024 * 1024)
    mb
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Fetching Block Tips
    """)
    return


@app.cell
def _(F, block_source, spark):
    (spark.read
        .option("mergeSchema",True)
        .table(block_source)
        .filter(f"date >= current_date-1")
        .select(F.max('number'))
        .show()
    )
    return


@app.cell
def _():
    # # optimize table 
    # _query   = f"""
    # OPTIMIZE {TABLE_NAME} ZORDER BY (number)
    # """
    # spark.sql(_query).show(truncate=False)
    return


@app.cell
def _(TABLE_NAME, current_date, spark):
    spark.table(TABLE_NAME)\
        .filter(f"date = '{current_date}'")\
        .printSchema()
    return


@app.cell
def _(TABLE_NAME, current_date, spark):
    # show table metrics like size  

    (spark.table(TABLE_NAME)
    .select("number",'transaction_count','mediantime','timestamp','last_modified')
    .filter(f"date = '{current_date}'")
    .orderBy("number", ascending=False)
    .show(truncate=False))
    return


@app.cell
def _(spark):
    # checking dedupes against the number/block_number 

    _q = f"""
    -- how many block numbers got re-published (rewritten) vs first-written
    SELECT
        count(DISTINCT number)                                                  AS distinct_blocks,
        count(*) - count(DISTINCT number)                                       AS republished_rows,
        count(*)                                                                AS total_rows,
        round((count(*) - count(DISTINCT number)) * 100.0 / count(*), 2)        AS pct_republished
    FROM workspace.btc_data.blocks 
    where date >= current_date-10

    """

    spark.sql(_q).show(truncate=False)
    return


@app.cell
def _(spark):
    # dedupe check. all blocks
    spark.sql("""
    SELECT count(*) - count(DISTINCT number) AS republished_rows,
           round((count(*) - count(DISTINCT number)) * 100.0 / count(*), 2) AS pct
    FROM workspace.btc_data.blocks;

    """).show()
    return


@app.cell
def _(spark):
    spark.conf.get("spark.sql.session.timeZone")
    return


@app.cell
def _(TABLE_NAME, spark):
    spark.sql(f"""

    select number,from_utc_timestamp(timestamp,'America/Toronto') as timestamp,timestamp as base_dttm,size,weight,transaction_count,date ,last_modified

    from {TABLE_NAME}
    where date >= current_date
    order by number desc


    """).show(truncate=False)
    return


@app.cell
def _(TABLE_NAME, spark):
    spark.sql(f"""
    SELECT
        number,
        timestamp AS raw_timestamp,
        from_utc_timestamp(timestamp, 'America/Toronto') AS toronto_timestamp,
        from_utc_timestamp(timestamp, 'EST') AS est_timestamp
    FROM {TABLE_NAME}
    where date >= current_date-12
    ORDER BY number DESC
    LIMIT 5
    """).show(truncate=False)
    return


@app.cell
def _():
    1
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Faster Scan for s3
    """)
    return


@app.cell
def _(TABLE_NAME, datetime, spark, timedelta):

    LOOKBACK_DAYS = 10
    REORG_BUFFER = 10
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d")
    S3_BLOCKS = "s3a://aws-public-blockchain/v1.0/btc/blocks"

    def get_latest_block_number_on_delta(spark, table_name: str):
        """
        Returns the latest block number from the specified Delta table.
        """
        q = f"""
        select max(number) as latest_block_number from {table_name}
        where date >= current_date-1
        """
        result = spark.sql(q).collect()[0]
        return result["latest_block_number"]

    def date_window(lookback_days: int = LOOKBACK_DAYS):
        """
        Returns a tuple of (start_date, end_date) for the given lookback_days.
        """
        now = datetime.now()
        end_date = now.strftime("%Y-%m-%d")
        start_date = (now + timedelta(days=-lookback_days)).strftime("%Y-%m-%d")
        return start_date, end_date

    def generate_s3_paths(base_url: str, start_date: str, end_date: str):
        """
        Generates exact partition folder URIs between start_date and end_date.
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        num_days = (end - start).days

        return [
            f"{base_url}/date={(start + timedelta(days=i)).strftime('%Y-%m-%d')}/"
            for i in range(num_days + 1)
        ]

    def read_blocks_s3(spark, start_date: str, end_date: str, latest_block: int = 0, REORG_BUFFER: int = REORG_BUFFER):
        """
        Reads blocks from S3 for the given date range and returns a Spark DataFrame.
        """
        latest_block = get_latest_block_number_on_delta(spark, TABLE_NAME) if latest_block == 0 else latest_block

        # 1. Generate exact partition directories (bypasses full bucket scan)
        target_paths = generate_s3_paths(S3_BLOCKS, start_date, end_date)

        # 2. Set options BEFORE calling .parquet()
        rel = (
            spark.read
            .option("basePath", S3_BLOCKS)
            .parquet(*target_paths)
            .filter(f'number > {latest_block - 100}')
        )

        return rel


    # Execution
    start_date, end_date = date_window(lookback_days=LOOKBACK_DAYS)
    latest_block = get_latest_block_number_on_delta(spark, TABLE_NAME) 

    df = read_blocks_s3(spark, start_date, end_date, latest_block=latest_block, REORG_BUFFER=REORG_BUFFER)
    df.show(truncate=False)
    return


@app.cell
def _(spark):
    # checking if the external table works

    spark.sql("SHOW TABLES IN workspace.btc_data;;").show(truncate=False)
    return


@app.cell
def _():
    # # speed comparison between public table vs external table

    # S3_BLOCKS


    # spark.sql(f""" 
    # select * from parquet.`{S3_BLOCKS}`
    # where date = current_date
    # limit 100
    # """).show()
    return


@app.cell
def _(spark):
    df_source = spark.sql(f""" 
    select number,date,transaction_count from workspace.btc_data.blocks_source
    where date >=  "2024-01-01"  
    order by number desc
    """)
    return (df_source,)


@app.cell
def _(df_source):
    df_source.printSchema()
    return


@app.cell
def _():
    return


@app.cell
def _(col, df_source, pl):
    pl_df = pl.from_arrow((df_source.select(["number","transaction_count","date"])
    .withColumn("tx_1k" ,col("transaction_count")/1e3)).toArrow())
    return


@app.cell
def _(spark):
    spark.sql("""
    select  
            max(number) blk_tip,
            from_utc_timestamp(max(timestamp) ,'America/Toronto') as time_toronto,
            max(timestamp) time_utc,
            max(date) 
    from
        workspace.btc_data.blocks_source 


    """).show()
    return


@app.cell
def _(spark):
    from pyspark.sql import functions as F

    tx_path_spark = "s3a://aws-public-blockchain/v1.0/btc/transactions/"

    spx =( spark.read.option("mergeSchema", "true") 
    .parquet(tx_path_spark) ) 
    spx.printSchema()
    return F, spx


@app.cell
def _(spx):
    spx[['outputs','inputs','txid']].show()
    return


@app.cell
def _(spark):
    tx_df = (spark.read 
        .option("mergeSchema", "true") 
        .table("workspace.btc_data.tx_source"))
    return (tx_df,)


@app.cell
def _(tx_df):
    tx_df[['outputs','txid','inputs']].show()
    return


@app.cell
def _(spark):
    spark.sql("""select * from workspace.btc_data.tx_source

     where date = current_date -5
     """).show()
    return


@app.cell
def _(spark):
    spark.table("workspace.btc_data.tx_source").printSchema()
    return


@app.cell
def _():
    # spark.sql("""
    #     SELECT 
    #         b.number AS block_number,
    #         b.transaction_count,
    #         b.size AS block_size,
    #         b.timestamp AS block_timestamp,
    #         t.txid,
    #         t.input_count,
    #         t.output_count,
    #         t.input_value,
    #         t.output_value,
    #         t.fee,
    #         size(t.inputs) AS inputs_array_len,
    #         size(t.outputs) AS outputs_array_len
    #     FROM workspace.btc_data.blocks_source b
    #     JOIN workspace.btc_data.tx_source t
    #         ON b.number = t.block_number
    #     WHERE 
    #     b.date >= '2026-08-01'
    #     AND t.date >= '2026-08-01'
    #     ORDER BY b.number DESC, t.txid
    # """).show(50, truncate=False)
    return


@app.cell
def _():
    1
    return


if __name__ == "__main__":
    app.run()
