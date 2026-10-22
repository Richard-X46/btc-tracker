"""Shared Databricks Spark session helper.

Auth order: on-Databricks runtime -> DATABRICKS_HOST/TOKEN env ->
Databricks CLI profile (DATABRICKS_PROFILE, default "Richard").

The session is created once and reused. Nothing connects at import time;
the first query triggers compute provisioning.
"""

from __future__ import annotations

import os
import time
from typing import Any

# Spark Connect hands back tz-naive datetimes in *process-local* wall time,
# so a Toronto laptop silently shifts every timestamp by 4-5h. Pin the
# process to UTC: naive == UTC wall everywhere (laptop, container, cluster).
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # optional, env can come from elsewhere
    pass

_spark: Any = None


def _force_utc(spark: Any) -> Any:
    """Pin session timezone to UTC so TIMESTAMP reads come back as true
    UTC instants (never naive wall-clock in some local zone)."""
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    except Exception:
        pass
    return spark


def get_spark() -> Any:
    """Return the shared Spark session, creating it on first call."""
    global _spark
    if _spark is not None:
        return _force_utc(_spark)
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):  # running ON Databricks
        from pyspark.sql import SparkSession

        _spark = SparkSession.builder.getOrCreate()
    else:  # local dev via Databricks Connect
        from databricks.connect import DatabricksSession

        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
        if host and token:
            builder = DatabricksSession.builder.remote(
                host=host, token=token
            ).serverless(True)
        else:
            builder = DatabricksSession.builder.profile(
                os.getenv("DATABRICKS_PROFILE", "Richard")
            ).serverless(True)
        _spark = builder.getOrCreate()
    return _force_utc(_spark)
