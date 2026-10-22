"""Read-only health checks for the ingestion pipeline.

Usage:
    python src/btc_tracker/ingestion/delta_health.py
    (or PYTHONPATH=src python -m btc_tracker.ingestion.delta_health)

Each check prints its result; one failing check doesn't abort the rest.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

try:
    from btc_tracker.spark import get_spark
except ImportError:  # allow running as a bare script without install
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from btc_tracker.spark import get_spark

CATALOG = "workspace"
SCHEMA = "btc_data"
TIMEZONE = "America/Toronto"


def check_tables(spark: Any) -> None:
    print("== tables ==")
    for t in spark.catalog.listTables(f"{CATALOG}.{SCHEMA}"):
        print(f"  {t.tableType:8} {t.name}")


def check_sizes(spark: Any) -> None:
    print("== sizes (MB) ==")
    for table in ("blocks", "transactions", "price_minute",
                  "gold_daily_fee_metrics"):
        try:
            nbytes = spark.sql(
                f"DESCRIBE DETAIL {CATALOG}.{SCHEMA}.{table}"
            ).select("sizeInBytes").collect()[0][0]
            print(f"  {table:24} {nbytes / 1e6:,.1f}")
        except Exception as exc:
            print(f"  {table:24} ERROR: {exc}")


def check_tips(spark: Any) -> None:
    print("== tips ==")
    for label, sql in (
        ("blocks.number",
         f"SELECT max(number) FROM {CATALOG}.{SCHEMA}.blocks "
         f"WHERE date >= current_date - 30"),
        ("tx.block_number",
         f"SELECT max(block_number) FROM {CATALOG}.{SCHEMA}.transactions "
         f"WHERE date >= current_date - 30"),
        ("price.candle",
         f"SELECT max(candle_start_ts) FROM {CATALOG}.{SCHEMA}.price_minute "
         f"WHERE date >= current_date - 30"),
    ):
        try:
            print(f"  {label:16} {spark.sql(sql).collect()[0][0]}")
        except Exception as exc:
            print(f"  {label:16} ERROR: {exc}")


def check_ranges(spark: Any) -> None:
    print("== time ranges ==")
    try:
        spark.sql(
            f"""
            SELECT 'price_minute' AS source,
                   min(candle_start_ts) AS start_ts,
                   max(candle_start_ts) AS end_ts
            FROM {CATALOG}.{SCHEMA}.price_minute
            UNION ALL
            SELECT 'transactions',
                   min(block_timestamp), max(block_timestamp)
            FROM {CATALOG}.{SCHEMA}.transactions
            UNION ALL
            SELECT 'blocks', min(timestamp), max(timestamp)
            FROM {CATALOG}.{SCHEMA}.blocks
            ORDER BY source
            """
        ).show(truncate=False)
    except Exception as exc:
        print(f"  ERROR: {exc}")


def check_columns(spark: Any) -> None:
    print("== columns ==")
    try:
        spark.sql(
            f"""
            SELECT table_name, column_name, data_type
            FROM {CATALOG}.information_schema.columns
            WHERE table_schema = '{SCHEMA}'
            ORDER BY table_name, ordinal_position
            """
        ).show(200, truncate=False)
    except Exception as exc:
        print(f"  ERROR: {exc}")


def main() -> None:
    spark = get_spark()
    spark.conf.set("spark.sql.session.timeZone", TIMEZONE)
    for check in (check_tables, check_sizes, check_tips,
                  check_ranges, check_columns):
        try:
            check(spark)
        except Exception as exc:  # noqa: BLE001 - keep going
            print(f"{check.__name__} FAILED: {exc}")
        print()


if __name__ == "__main__":
    main()
