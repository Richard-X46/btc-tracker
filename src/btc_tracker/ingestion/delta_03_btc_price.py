import os
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import requests
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, DoubleType
from delta.tables import DeltaTable
from pyspark.sql import functions as F
import time

import logfire
from logging import basicConfig


load_dotenv() 

PRODUCT = "BTC-USD"
BASE = f"https://api.exchange.coinbase.com/products/{PRODUCT}/candles"
TABLE = "workspace.btc_data.price_minute"


SCHEMA = StructType([
    StructField("candle_start_ts", TimestampType(), False),
    StructField("date", StringType(), False),
    StructField("hour", IntegerType(), True),
    StructField("minute", IntegerType(), True),
    StructField("low", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("open", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("source", StringType(), True),
    StructField("ingested_at", TimestampType(), True),
])


def get_spark():
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):  # running ON Databricks
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    else:  # local dev via Databricks Connect
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.remote(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")
        ).serverless(True).getOrCreate()
    return spark


def ensure_price_table(spark):
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {TABLE} (
        candle_start_ts TIMESTAMP, date STRING, hour INT, minute INT,
        low DOUBLE, high DOUBLE, open DOUBLE, close DOUBLE,
        volume DOUBLE, source STRING, ingested_at TIMESTAMP
      ) USING DELTA PARTITIONED BY (date)
    """)

def get_logfire_token(spark):
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        return dbutils.secrets.get(scope="btc-tracker", key="logfire-token")
    else:
        return os.getenv("LOGFIRE_TOKEN")

# there is a cap of 300 candles per request
def fetch_candles(start: datetime, end: datetime, granularity: int = 60):
    """Pull [time, low, high, open, close, volume] buckets."""
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "granularity": granularity,
    }
    headers = {}
    if os.getenv("COINBASE_KEY"):
        headers["CB-ACCESS-KEY"] = os.getenv("COINBASE_KEY")
    r = requests.get(BASE, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    rows = []
    for t, low, high, o, c, v in r.json():
        ts = datetime.fromtimestamp(t, tz=timezone.utc)
        rows.append({
            "candle_start_ts": ts,
            "date": ts.strftime("%Y-%m-%d"),
            "hour": ts.hour, "minute": ts.minute,
            "low": low, "high": high,
            "open": o, "close": c, "volume": v,
        })
    return sorted(rows, key=lambda x: x["candle_start_ts"])

def to_price_df(spark,rows):

    if rows:
        return (spark.createDataFrame(rows,schema=SCHEMA)
            .withColumn("source", F.lit("coinbase"))
            .withColumn("ingested_at", F.current_timestamp()))


def merge_price(spark, df, table_name: str = TABLE):
    DeltaTable.forName(spark, table_name).alias("t").merge(
        df.alias("s"),
        "t.candle_start_ts = s.candle_start_ts"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def sync_price_range(spark, start_dttm, end_dttm):
    """Fetch Coinbase minute candles in [start, end) and merge them in."""
    cur = start_dttm.replace(second=0, microsecond=0)
    if cur.tzinfo is None:
        cur = cur.replace(tzinfo=timezone.utc)
    end = end_dttm.replace(second=0, microsecond=0)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    while cur < end:
        nxt = min(cur + timedelta(minutes=300), end)
        rows = fetch_candles(cur, nxt, 60)
        logfire.info("price sync", start=str(cur), end=str(nxt),
                     rows=len(rows))
        time.sleep(0.4)  # be nice to the public API
        if rows:
            merge_price(spark, to_price_df(spark, rows))
            cur = max(r["candle_start_ts"] for r in rows) + timedelta(minutes=1)
        else:
            cur = nxt


@logfire.instrument("Incremental price_minute sync from Coinbase")
def run_incremental(spark, lookback_hours: int = 2):
    """Top up price_minute from its tip to now (backfill covers history)."""
    ensure_price_table(spark=spark)
    max_candle_ts = spark.sql(f"""
                        select
                        max(candle_start_ts) as max_ts
                        from {TABLE}""").collect()[0]['max_ts']

    if max_candle_ts is None:
        start_dttm = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    else:
        start_dttm = max_candle_ts + timedelta(minutes=1)
        if start_dttm.tzinfo is None:
            start_dttm = start_dttm.replace(tzinfo=timezone.utc)

    end = datetime.now(timezone.utc)
    if start_dttm >= end - timedelta(minutes=1):
        logfire.info("price_minute already fresh",
                     max_candle_ts=str(max_candle_ts))
        return
    sync_price_range(spark, start_dttm, end)

@logfire.instrument("Backfill price_minute from Coinbase")
def backfill_btc_price_minutely(spark):

     # get timestamp from tx starting dttm
    tx_start_dttm = spark.sql("""
        select 
            min(block_timestamp) as tx_start_block_dttm,
            max(block_timestamp) as tx_latest_block_dttm
        from 
            workspace.btc_data.transactions""").collect()[0]["tx_start_block_dttm"]
     # creating the table for minutely price if not exists
    ensure_price_table(spark=spark)

    # check max candle ts on the table
    max_candle_ts = spark.sql(f"""
                        select 
                        max(candle_start_ts) as max_ts
                        from {TABLE}""").collect()[0]['max_ts']

    if max_candle_ts is None:
        # table is empty use the tx table start horizon
        start_dttm = tx_start_dttm
       

    else:
        # table has data use the max time + 1 min td for the pull
        start_dttm = max_candle_ts + timedelta(minutes= 1 )

    sync_price_range(spark, start_dttm, datetime.now(timezone.utc))

    

def main():
    mode = os.getenv("INGESTION_MODE", "incremental")  
    spark = get_spark()
    token = get_logfire_token(spark)
    if token:
        os.environ["LOGFIRE_TOKEN"] = token
    logfire.configure(service_name="btc_data.price_minute ingestion service")
    basicConfig(handlers=[logfire.LogfireLoggingHandler()])


    if mode == "backfill":
        backfill_btc_price_minutely(spark)
    elif mode == "incremental":
        run_incremental(spark)
    else:
        raise ValueError(f"Unknown INGESTION_MODE: {mode}")


if __name__ == "__main__":
    main()