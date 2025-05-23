"""
Bitcoin Block Data Fetcher

This module contains prefect tasks for fetching Bitcoin block data from blockchair.com.

Context for copilot:
- This is a work in progress
- Current goal: Understanding data schema for PostgreSQL table design
- Will be deployed as a Prefect flow on AWS Lightsail
- Needs to handle both historical data and daily updates
- Data source: gz.blockchair.com/bitcoin/blocks/

Technical requirements:
- Prefect for workflow orchestration
- PostgreSQL for data storage
- Needs to handle gzipped TSV files
"""
import requests
from prefect import flow, task
import pandas as pd
from io import BytesIO
import prefect
from datetime import datetime


@task(name= 'fetch_block_data',retries=3, retry_delay_seconds=5,log_prints=True)
def fetch_block_data(url: str):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    return response.content

@flow(name="download_block_data_flow", log_prints=True)
def download_block_data_flow():
    """Test flow for downloading block data"""
    url = "https://gz.blockchair.com/bitcoin/blocks/blockchair_bitcoin_blocks_20090519.tsv.gz"
    data = fetch_block_data(url)
    df = pd.read_csv(BytesIO(data), sep="\t", compression="gzip")
    prefect.get_run_logger().info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")


    return df

if __name__ == "__main__":
    run_name = f"daily_block_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    download_block_data_flow()


