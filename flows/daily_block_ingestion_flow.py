from prefect import flow, get_run_logger
from datetime import date, timedelta
from flows.block_ingestion_flow import download_for_date

@flow(name="daily_block_ingest", log_prints=True)
def daily_block_ingest():
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
    logger = get_run_logger()
    logger.info(f"Running daily ingest for {yesterday}")
    count = download_for_date(yesterday)
    logger.info(f"Ingested {count} rows for {yesterday}")

if __name__ == "__main__":
    daily_block_ingest()



