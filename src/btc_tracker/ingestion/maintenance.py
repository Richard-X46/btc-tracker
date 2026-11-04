"""Manual maintenance ops: OPTIMIZE/VACUUM + freshness gap checks.

Execute from ipython, e.g.:
    from btc_tracker.ingestion.maintenance import get_spark, check_missing_blocks
    spark = get_spark()
    check_missing_blocks(spark)

Nothing runs on import. No single entrypoint at the bottom on purpose.
"""
import os
from dotenv import load_dotenv
import pyarrow.dataset as ds
import pyarrow.fs as pafs
from pyspark.sql import functions as F
from btc_tracker.ingestion.delta_01_blocks import merge_blocks, dedupe_blocks
from btc_tracker.ingestion.delta_02_transactions import merge_tx, KEEP_COLS
from btc_tracker.ingestion.delta_03_btc_price import sync_price_range, ensure_price_table

load_dotenv()

BLOCKS_TABLE = "workspace.btc_data.blocks"
TX_TABLE = "workspace.btc_data.transactions"
PRICE_TABLE = "workspace.btc_data.price_minute"


def get_spark():
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):  # running ON Databricks
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    else:  # local dev via Databricks Connect
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.remote(
            host=os.getenv("DATABRICKS_HOST",""),
            token=os.getenv("DATABRICKS_TOKEN","")
        ).serverless(True).getOrCreate()
    return spark


# ----/// OPTIMIZE ///----
# NOTE: blocks/transactions are Liquid-clustered (blocks on number).
# Liquid tables take plain OPTIMIZE (compaction honors clustering);
# ZORDER BY errors on them, so no ZORDER here.
# Alternative: ALTER TABLE ... SET TBLPROPERTIES (clusteringAutoEnabled = true)
# and drop manual OPTIMIZE entirely.
def optimize_table(spark, table: str):
    """Coalesce tiny files. Run after backfills or when Sharing reads slow down."""
    spark.sql(f"OPTIMIZE {table}")
    print(f"optimized {table}")


def optimize_blocks(spark):
    return optimize_table(spark, BLOCKS_TABLE)


def optimize_transactions(spark):
    return optimize_table(spark, TX_TABLE)


def optimize_price(spark):
    return optimize_table(spark, PRICE_TABLE)


#running optimizations for tables

optimize_blocks(get_spark())

optimize_transactions(get_spark())

optimize_price(get_spark())




# ----/// VACUUM ///----
def vacuum_table(spark, table: str, retain_hours: int = 168):
    """Drop stale files older than retention. 7d default keeps time-travel safe."""
    spark.sql(f"VACUUM {table} RETAIN {retain_hours} HOURS")
    print(f"vacuumed {table} retain={retain_hours}h")


def vacuum_blocks(spark, retain_hours: int = 168):
    return vacuum_table(spark, BLOCKS_TABLE, retain_hours)


def vacuum_transactions(spark, retain_hours: int = 168):
    return vacuum_table(spark, TX_TABLE, retain_hours)


def vacuum_price(spark, retain_hours: int = 168):
    return vacuum_table(spark, PRICE_TABLE, retain_hours)


spark=get_spark()
# running vacuums for tables
vacuum_blocks(spark)
vacuum_transactions(spark)
vacuum_price(spark)








# ----/// GAP CHECK: blocks ///----
def check_missing_blocks(spark):
    """Full-history scan: block numbers in range but missing rows. Empty == healthy."""
    gaps = spark.sql(f"""
        with ordered as (
            select number,
                   lead(number) over (order by number) as nxt
            from {BLOCKS_TABLE}
        )
        select number + 1 as gap_start, nxt - 1 as gap_end
        from ordered
        where nxt is not null and nxt - number > 1
        order by gap_start
    """).collect()
    print(f"block gaps (full history): {gaps if gaps else 'none'}")
    return gaps


# ----/// GAP CHECK: tx blocks ///----
def check_missing_tx_blocks(spark, lookback_days: int = 300):
    """Blocks that exist but have zero transactions (excluding template lag)."""
    missing = spark.sql(f"""
        select b.number as missing_block
        from (select distinct number from {BLOCKS_TABLE}
              where date >= current_date - {lookback_days}) b
        left join (select distinct block_number from {TX_TABLE}
                   where date >= current_date - {lookback_days}) t
          on t.block_number = b.number
        where t.block_number is null
        order by missing_block
    """).collect()
    print(f"blocks without tx (last {lookback_days}d): "
          f"{[r['missing_block'] for r in missing] if missing else 'none'}")
    return missing


# ----/// GAP CHECK: minutely price ///----
def check_missing_price_minutes(spark, lookback_days: int = 300):
    """Minute candles with gaps > 60s. Crypto trades 24/7 so any gap is missing data."""
    gaps = spark.sql(f"""
        with ordered as (
            select candle_start_ts,
                   lead(candle_start_ts) over (order by candle_start_ts) as nxt
            from {PRICE_TABLE}
            where date >= current_date - {lookback_days}
        )
        select candle_start_ts as gap_start, nxt as gap_end,
               (unix_timestamp(nxt) - unix_timestamp(candle_start_ts)) / 60 as missing_minutes
        from ordered
        where nxt is not null
          and unix_timestamp(nxt) - unix_timestamp(candle_start_ts) > 60
        order by gap_start
    """).collect()
    print(f"price minute gaps (last {lookback_days}d): {gaps if gaps else 'none'}")
    return gaps



# fix gaps in blocks manuall 


# ----/// BACKFILL: plug block gaps ///----
def backfill_block_gaps(spark, gaps):
    """Plug caller-supplied gaps: fetch exactly those heights from public S3,
    upsert into the blocks table. No date-window filter on what to merge —
    the gap list itself is the filter.

    Usage:
        block_gaps = check_missing_blocks(spark)
        backfill_block_gaps(spark, block_gaps)
    """
    numbers = []
    for r in gaps:
        numbers.extend(range(r["gap_start"], r["gap_end"] + 1))
    if not numbers:
        print("no gaps to plug")
        return []

    # boundary dates around the gaps so we read 1-2 S3 partitions, not everything
    bounds = spark.sql(f"""
        select min(date) as dmin, max(date) as dmax from {BLOCKS_TABLE}
        where number in ({numbers[0] - 1}, {numbers[-1] + 1})
    """).collect()[0]
    s3 = pafs.S3FileSystem(anonymous=True, region="us-east-2")
    dataset = ds.dataset(
        "aws-public-blockchain/v1.0/btc/blocks/",
        filesystem=s3,
        partitioning="hive",
    )
    paths = sorted({
        f.path.rsplit("/", 1)[0] + "/"
        for f in dataset.get_fragments(
            filter=(ds.field("date") >= str(bounds["dmin"]))
            & (ds.field("date") <= str(bounds["dmax"]))
        )
    })
    df = (
        spark.read.option("mergeSchema", "true")
        .option("basePath", "s3://aws-public-blockchain/v1.0/btc/blocks")
        .parquet(*[f"s3://{p}" for p in paths])
        .filter(F.col("number").isin(numbers))
    )
    df = dedupe_blocks(df)
    found = sorted(r["number"] for r in df.select("number").distinct().collect())
    if not found:
        print(f"gap heights not in public S3 yet: {numbers}")
        return []
    merge_blocks(spark, df, BLOCKS_TABLE)
    print(f"plugged blocks: {found}")
    return found


block_gaps = check_missing_blocks(spark)
backfill_block_gaps(spark, block_gaps)




# ----/// BACKFILL: plug tx gaps ///----
def backfill_tx_blocks(spark, missing):
    """Plug caller-supplied missing blocks: fetch exactly those block_numbers
    from public S3 transactions, upsert into the transactions table.

    Usage:
        missing = check_missing_tx_blocks(spark)
        backfill_tx_blocks(spark, missing)
    """
    numbers = sorted({r["missing_block"] for r in missing})
    if not numbers:
        print("no tx gaps to plug")
        return []
    bounds = spark.sql(f"""
        select min(date) as dmin, max(date) as dmax from {BLOCKS_TABLE}
        where number in ({", ".join(map(str, numbers))})
    """).collect()[0]
    dataset = ds.dataset(
        "aws-public-blockchain/v1.0/btc/transactions/",
        filesystem=pafs.S3FileSystem(anonymous=True, region="us-east-2"),
        partitioning="hive",
    )
    paths = sorted({
        f.path.rsplit("/", 1)[0] + "/"
        for f in dataset.get_fragments(
            filter=(ds.field("date") >= str(bounds["dmin"]))
            & (ds.field("date") <= str(bounds["dmax"]))
        )
    })
    df = (
        spark.read.option("mergeSchema", "true")
        .option("basePath", "s3://aws-public-blockchain/v1.0/btc/transactions")
        .parquet(*[f"s3://{p}" for p in paths])
        .filter(F.col("block_number").isin(numbers))
        .select([*KEEP_COLS])
    )
    found = sorted(r["block_number"] for r in df.select("block_number").distinct().collect())
    if not found:
        print(f"tx for blocks not in public S3 yet: {numbers}")
        return []
    merge_tx(spark, df, TX_TABLE)
    print(f"plugged tx blocks: {found}")
    return found

# checking tx
gaps = check_missing_tx_blocks(spark, lookback_days=300)

backfill_tx_blocks(spark, gaps)






# ----/// BACKFILL: plug price gaps ///----
def backfill_price_gaps(spark, gaps):
    """Plug caller-supplied minute gaps from Coinbase, ±1 min margin each side.

    Usage:
        gaps = check_missing_price_minutes(spark)
        backfill_price_gaps(spark, gaps)
    """
    from datetime import timedelta

    if not gaps:
        print("no price gaps to plug")
        return []
    ensure_price_table(spark)
    plugged = []
    for r in gaps:
        start = r["gap_start"] - timedelta(minutes=1)
        end = r["gap_end"] + timedelta(minutes=2)
        sync_price_range(spark, start, end)
        plugged.append((str(start), str(end)))
    print(f"plugged price ranges: {plugged}")
    return plugged

gaps = check_missing_price_minutes(spark, lookback_days=300)
backfill_price_gaps(spark, gaps)


# ----/// DELETION VECTORS ///----
# DV flags block raw-REST Sharing reads (parquet format can't resolve them).
# Our MERGEs are insert-only so no live DVs exist; safe to switch off + purge.
def disable_deletion_vectors(spark, table: str):
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES (delta.enableDeletionVectors = false)")
    print(f"deletion vectors disabled on {table}")


def purge_deletion_vectors(spark, table: str):
    spark.sql(f"REORG TABLE {table} APPLY (PURGE)")
    print(f"deletion vectors purged from {table}")


def drop_deletion_vectors_transactions(spark):
    disable_deletion_vectors(spark, TX_TABLE)
    purge_deletion_vectors(spark, TX_TABLE)


# ----/// GOLD TABLES: sharing-ready (no DVs) ///----
# Open sharing serves raw parquet and 400s on deletion vectors
# (DS_UNSUPPORTED_DELTA_TABLE_FEATURES). dbt models now carry
# tblproperties to stay DV-free; these functions fix existing tables.
# Manual by design: purge rewrites files, must not run on import.
GOLD_TABLES = [
    "workspace.btc_data.gold_daily_fee_metrics",
    "workspace.btc_data.gold_daily_whale_activity",
    "workspace.btc_data.gold_daily_miner_revenue",
    "workspace.btc_data.gold_daily_block_intervals",
    "workspace.btc_data.gold_daily_fee_tiers",
    "workspace.btc_data.gold_weekly_fee_heatmap",
]


def disable_gold_deletion_vectors(spark):
    for t in GOLD_TABLES:
        disable_deletion_vectors(spark, t)


def purge_gold_deletion_vectors(spark):
    for t in GOLD_TABLES:
        purge_deletion_vectors(spark, t)


def maintain_gold_sharing(spark):
    """Disable + purge DVs on all gold tables. Run once, or after any
    full-refresh that re-enables them.

    Usage:
        from btc_tracker.ingestion.maintenance import get_spark, maintain_gold_sharing
        spark = get_spark()
        maintain_gold_sharing(spark)
    """
    disable_gold_deletion_vectors(spark)
    purge_gold_deletion_vectors(spark)


# ----/// GOLD TABLES: ownership grants ///----
# Prod-job-built tables are owned by the service principal; laptop-side
# identities need explicit MODIFY. Must run as owner/admin (SQL editor or a
# prod job as the SP) — a plain Connect session as a non-owner fails.
GOLD_GRANT_PRINCIPAL = "richardpears46@gmail.com"


def grant_gold_modify(spark, principal: str = GOLD_GRANT_PRINCIPAL):
    """GRANT MODIFY on all gold tables to `principal`. Run once as owner/admin,
    and again whenever a new gold table is added to GOLD_TABLES.

    Usage:
        from btc_tracker.ingestion.maintenance import get_spark, grant_gold_modify
        spark = get_spark()
        grant_gold_modify(spark)
    """
    for t in GOLD_TABLES:
        spark.sql(f"GRANT MODIFY ON TABLE {t} TO `{principal}`")
    print(f"modify granted on {len(GOLD_TABLES)} gold tables to {principal}")


grant_gold_modify(get_spark(), principal=GOLD_GRANT_PRINCIPAL)

# fixing tx table deletion vectors
drop_deletion_vectors_transactions(spark)






# ----/// LIQUID CLUSTERING MIGRATION ///----
# One-shot rewrites: pause the hourly job first, resume after.
# DVs are explicitly OFF (default-on would re-block raw-REST Sharing reads).
# Row tracking stays on (harmless hidden cols; readers use union_by_name).
def migrate_transactions_to_liquid(spark):
    """Full rewrite of tx, clustered by date then block_number. ~10GB, minutes, one time.
    Date first so Sharing date predicates actually prune; block_number second
    so merge windows still prune."""
    spark.sql("""
        CREATE OR REPLACE TABLE workspace.btc_data.transactions
        CLUSTER BY (date, block_number)
        TBLPROPERTIES (delta.enableDeletionVectors = false)
        AS SELECT * FROM workspace.btc_data.transactions
    """)
    print("transactions: liquid clustered on (date, block_number), DVs off")


def migrate_price_to_liquid(spark):
    """Full rewrite of price_minute, clustered by candle_start_ts. Tiny table."""
    spark.sql("""
        CREATE OR REPLACE TABLE workspace.btc_data.price_minute
        CLUSTER BY (candle_start_ts)
        TBLPROPERTIES (delta.enableDeletionVectors = false)
        AS SELECT * FROM workspace.btc_data.price_minute
    """)
    print("price_minute: liquid clustered on candle_start_ts, DVs off")


def tx_liquid_alter(spark):
    """ALTER TABLE to liquid-cluster tx, no rewrite. ~10GB, minutes."""
    spark.sql("""
       ALTER TABLE workspace.btc_data.transactions CLUSTER BY (date, block_number);
    """)
    spark.sql(
        """        OPTIMIZE workspace.btc_data.transactions;
"""
    )
    print("transactions: liquid clustered on date,block_number, run OPTIMIZE to coalesce files")

migrate_price_to_liquid(spark)


migrate_transactions_to_liquid(spark)
tx_liquid_alter(spark)

if __name__ == "__main__":
    pass
