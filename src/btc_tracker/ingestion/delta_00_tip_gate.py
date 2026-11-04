# tip gate for the failsafe schedule: checks if public S3 has blocks
# newer than the local Delta table, then publishes `has_new` for
# downstream tasks to gate on via RunIf conditions.
#
# CALL GRAPH
# main()
# ├── get_spark()
# ├── get_local_tip(spark)          # max(number) from workspace.btc_data.blocks
# ├── get_public_tip()              # max(number) from public S3, pyarrow anonymous
# └── publish(has_new)              # dbutils.jobs.taskValues.set("has_new", ...)
#
# Fail-open: if the public tip can't be read, publishes "true" so the
# hourly failsafe still runs the merges instead of silently skipping.
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logfire
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as pafs

load_dotenv()

TABLE_NAME = "workspace.btc_data.blocks"
LOOKBACK_DAYS = 3


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


def get_local_tip(spark) -> int:
    row = spark.sql(f"""
        select max(number) as tip
        from {TABLE_NAME}
        where date >= current_date - 5
    """).collect()[0]["tip"]
    return row or 0


def get_public_tip(start_date: str) -> int | None:
    """Max block number in the public bucket since start_date (pyarrow, no Spark)."""
    s3 = pafs.S3FileSystem(anonymous=True, region="us-east-2")
    dataset = ds.dataset(
        "aws-public-blockchain/v1.0/btc/blocks/",
        filesystem=s3,
        partitioning="hive",
    )
    table = dataset.scanner(
        columns=["number"],
        filter=(ds.field("date") >= start_date),
    ).to_table()
    if table.num_rows == 0:
        return None
    return pc.max(table["number"]).as_py()


def publish(has_new: bool) -> None:
    value = "true" if has_new else "false"
    try:
        from databricks.sdk.runtime import dbutils
        dbutils.jobs.taskValues.set("has_new", value)
    except Exception:  # local dev has no jobs runtime; log and continue
        print(f"has_new={value} (taskValues unavailable locally)")
    logfire.info("tip_gate", has_new=has_new)


def main() -> None:
    spark = get_spark()
    try:
        from pyspark.dbutils import DBUtils
        token = DBUtils(spark).secrets.get(scope="btc-tracker", key="logfire-token")
    except Exception:
        token = os.getenv("LOGFIRE_TOKEN")
    if token:
        os.environ["LOGFIRE_TOKEN"] = token
    logfire.configure(service_name="btc_data.tip_gate")

    start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    local_tip = get_local_tip(spark)
    try:
        public_tip = get_public_tip(start_date)
    except Exception as e:
        logfire.error("tip_gate public read failed, failing open", error=str(e))
        public_tip = None

    # Fail-open: unknown public tip means run the merges.
    has_new = public_tip is None or public_tip > local_tip
    logfire.info(
        "tip_gate tips",
        local_tip=local_tip,
        public_tip=public_tip,
        has_new=has_new,
    )
    publish(has_new)


if __name__ == "__main__":
    main()
