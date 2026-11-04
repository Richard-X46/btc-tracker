import asyncio
from datetime import datetime, timedelta, timezone
import sys
import time
import duckdb
import logfire

BUCKET = "s3://aws-public-blockchain/v1.0/btc/blocks"

# Highest tip already handed to Databricks. Monotonic guard so init/want
# resends and reconnect replays never double-trigger the job.
last_triggered = 0


def public_tip(dates):
    """Max block number across date partitions. Immune to per-height vs
    compacted part-* file layout: duckdb globs whatever files exist."""
    con = duckdb.connect()
    con.execute("SET s3_region='us-east-2'")
    # Force anonymous: process may carry unrelated AWS keys (via .env)
    # that duckdb would otherwise sign with and get 403.
    con.execute("SET s3_access_key_id=''")
    con.execute("SET s3_secret_access_key=''")
    con.execute("SET s3_session_token=''")
    files = [f"{BUCKET}/date={d}/*.snappy.parquet" for d in dates]
    try:
        row = con.execute(
            """SELECT max(number) FROM 
                        read_parquet(?, hive_partitioning=1)""",
            [files],
        ).fetchone()
    except duckdb.IOException:
        return None  # no files under these partitions yet
    return row[0] if row else None


def recent_dates(ts, days=2):
    base = datetime.fromtimestamp(ts, timezone.utc).date()
    return [(base - timedelta(days=i)).isoformat() for i in range(days)]


async def poll_s3(h, ts, trigger_job):
    """Trigger Databricks once public S3 has tip h.

    Returns the run_id when triggered, None on timeout (next tip covers gap)
    or when already triggered. Blocking duckdb call runs in a thread so the
    socket loop stays free.
    """
    global last_triggered
    if h is None or h <= last_triggered:
        return None
    dates = recent_dates(ts)
    start = time.monotonic()
    attempts = 0
    with logfire.span("s3.poll", height=h, partitions=dates):
        for wait in [0, 60, 180, 600]:
            await asyncio.sleep(wait)
            attempts += 1
            tip = await asyncio.to_thread(public_tip, dates)
            if tip is not None and tip >= h:
                run_id = await trigger_job(h)
                last_triggered = h
                logfire.info(
                    "s3.poll.found",
                    height=h,
                    public_tip=tip,
                    attempts=attempts,
                    duration_s=round(time.monotonic() - start, 1),
                    run_id=run_id,
                )
                return run_id
    logfire.info(
        "s3.poll.miss",
        height=h,
        attempts=attempts,
        duration_s=round(time.monotonic() - start, 1),
    )
    print(f"S3 miss for tip {h}")
    return False


if __name__ == "__main__":

    stamp = float(sys.argv[1]) if len(sys.argv) > 1 else time.time()
    print(public_tip(recent_dates(stamp)))
