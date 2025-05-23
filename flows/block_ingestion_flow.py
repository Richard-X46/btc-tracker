from prefect import flow, get_run_logger
from io import BytesIO
from tasks.block_data import fetch_block_data
from backend.db.database import engine
from backend.db.crud import insert_blocks_skip_dups
import pandas as pd
from datetime import datetime, timedelta

@flow(name="download_blockchair_data_for_date", log_prints=True)
def download_for_date(date_str: str):
    url = f"https://gz.blockchair.com/bitcoin/blocks/blockchair_bitcoin_blocks_{date_str}.tsv.gz"
    logger = get_run_logger()
    logger.info(f"Downloading {url}")
    data = fetch_block_data(url)
    df = pd.read_csv(BytesIO(data), sep="\t", compression="gzip")
    with engine.begin() as conn:
        insert_blocks_skip_dups(conn, df.to_dict(orient="records"))
    logger.info(f"Finished ingesting {len(df)} rows for {date_str}")
    return len(df)



@flow(name="historical_backfill", log_prints=True)
def historical_backfill(
    start_date: str = "2009-01-03",
    end_date: str | None = None,
    max_concurrent: int = 5,
):


    if end_date is None:
        end_date = (datetime.utcnow().date() - timedelta(days=1)).isoformat()

    dates = pd.date_range(start=start_date, end=end_date).strftime("%Y%m%d").tolist()

    download_for_date.with_options(max_concurrent_tasks=max_concurrent).map(dates)

if __name__ == "__main__":
    historical_backfill()