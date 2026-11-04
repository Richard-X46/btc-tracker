"""Combined merge task: blocks + transactions + price in ONE Spark session.

One task = one serverless container = one cold start, instead of three.
Calls the existing run_incremental() functions in order; those files are
untouched. Depends on check_tip/run_gate in the job yml like before.
"""
import os
import sys
from dotenv import load_dotenv
from logging import basicConfig
import logfire

# Serverless tasks can't take task-level libraries, so the package isn't
# installed at runtime (and __file__ is undefined under exec).
# Walk up from cwd to find the synced <bundle-files>/src dir.
_pkg_root = None
_d = os.getcwd()
for _ in range(8):
    if os.path.isfile(os.path.join(_d, "src", "btc_tracker", "ingestion", "__init__.py")):
        _pkg_root = os.path.join(_d, "src")
        break
    _parent = os.path.dirname(_d)
    if _parent == _d:
        break
    _d = _parent
if _pkg_root is not None and _pkg_root not in sys.path:
    sys.path.insert(0, _pkg_root)

from btc_tracker.ingestion.delta_01_blocks import (
    get_spark,
    get_logfire_token,
    run_incremental as run_blocks,
    TABLE_NAME as BLOCKS_TABLE,
)
from btc_tracker.ingestion.delta_00_tip_gate import (
    get_local_tip,
    get_public_tip,
    LOOKBACK_DAYS,
)
from btc_tracker.ingestion.delta_02_transactions import run_incremental as run_tx
from btc_tracker.ingestion.delta_03_btc_price import run_incremental as run_price
from btc_tracker.ingestion.live_gold import run_live_blocks

load_dotenv()


@logfire.instrument("Combined incremental merge: blocks + tx + price")
def main():
    import datetime

    spark = get_spark()
    token = get_logfire_token(spark)
    if token:
        os.environ["LOGFIRE_TOKEN"] = token
    logfire.configure(service_name="btc_data.merges")
    basicConfig(handlers=[logfire.LogfireLoggingHandler()])

    # gate (was check_tip + run_gate tasks): skip everything when fresh
    start_date = (datetime.datetime.now() - datetime.timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    local_tip = get_local_tip(spark)
    try:
        public_tip = get_public_tip(start_date)
    except Exception as e:
        logfire.error("tip_gate public read failed, failing open", error=str(e))
        public_tip = None
    if public_tip is not None and public_tip <= local_tip:
        logfire.info("tip_gate skip: no new blocks", local_tip=local_tip, public_tip=public_tip)
        print(f"tip_gate skip: local={local_tip} public={public_tip}")
        return

    with logfire.span("stage.blocks"):
        run_blocks(spark, BLOCKS_TABLE)
    with logfire.span("stage.transactions"):
        run_tx(spark)
    with logfire.span("stage.price"):
        run_price(spark)
    with logfire.span("stage.live"):
        run_live_blocks(spark)

    logfire.info("combined merges done")


if __name__ == "__main__":
    main()
