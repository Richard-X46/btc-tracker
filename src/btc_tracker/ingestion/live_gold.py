"""Live gold: block-grain feed for the realtime landing views.

Spark-owned table (like price_minute): created + maintained here, never by dbt.
Runs per block event from pipeline_merges (stage.live), downstream of price.

Table (live_ prefix = live/minutely cadence, short retention):
- live_block_feed: trailing block window feeding Graph 1 (pace) + Graph 2
  (last-N value+price). Query holds ~120 blocks; dashboard selects N.
  DVs off from birth (open sharing requirement).
"""

LIVE_BLOCK_FEED = "workspace.btc_data.live_block_feed"
BLOCKS_TABLE = "workspace.btc_data.blocks"
TX_TABLE = "workspace.btc_data.transactions"
PRICE_TABLE = "workspace.btc_data.price_minute"
HOLD_BLOCKS = 120


def ensure_live_block_feed(spark):
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {LIVE_BLOCK_FEED} (
        number BIGINT, timestamp TIMESTAMP, tx_count BIGINT,
        inputs BIGINT, outputs BIGINT, value_btc DOUBLE, fees_btc DOUBLE,
        ivl_s DOUBLE, btc_usd DOUBLE
      ) USING DELTA TBLPROPERTIES (delta.enableDeletionVectors = false)
    """)


def run_live_blocks(spark, hold: int = HOLD_BLOCKS):
    """Rebuild the trailing block window and merge into live_block_feed."""
    import logfire

    ensure_live_block_feed(spark)
    tip = spark.sql(f"select max(number) as tip from {BLOCKS_TABLE}").collect()[0]["tip"]
    if tip is None:
        logfire.warn("live feed skipped: blocks empty")
        return
    floor = tip - hold

    spark.sql(f"""
      CREATE OR REPLACE TEMP VIEW live_blk AS
      select number, timestamp
      from {BLOCKS_TABLE} where number > {floor - 1} order by number
    """)
    spark.sql(f"""
      CREATE OR REPLACE TEMP VIEW live_tx AS
      select block_number,
             count(*) as tx_count,
             sum(input_count) as inputs,
             sum(output_count) as outputs,
             sum(output_value) as value_btc,
             sum(fee) as fees_btc
      from {TX_TABLE}
      where is_coinbase = false and block_number > {floor - 1}
      group by block_number
    """)
    spark.sql(f"""
      CREATE OR REPLACE TEMP VIEW live_px AS
      select candle_start_ts as m, close as btc_usd
      from {PRICE_TABLE}
      where candle_start_ts >= current_timestamp() - INTERVAL 3 DAYS
    """)
    spark.sql(f"""
      MERGE INTO {LIVE_BLOCK_FEED} t
      USING (
        with j as (
          select b.number, b.timestamp, x.tx_count, x.inputs, x.outputs,
                 x.value_btc, x.fees_btc, p.btc_usd
          from live_blk b
          left join live_tx x on x.block_number = b.number
          left join live_px p on p.m = date_trunc('minute', b.timestamp)
        )
        select number, timestamp, tx_count, inputs, outputs, value_btc, fees_btc,
               unix_timestamp(timestamp) - unix_timestamp(lag(timestamp) over (order by number)) as ivl_s,
               btc_usd
        from j
      ) s ON t.number = s.number
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
    spark.sql(f"DELETE FROM {LIVE_BLOCK_FEED} WHERE number <= {floor}")
    n = spark.sql(f"select count(*) as n from {LIVE_BLOCK_FEED}").collect()[0]["n"]
    logfire.info("live feed merged", tip=tip, rows=n)
    print(f"live feed merged through block {tip} ({n} rows held)")
