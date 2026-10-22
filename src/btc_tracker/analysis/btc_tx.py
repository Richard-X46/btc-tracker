# public S3 bucket - btc tx and block data - ingestion scanning
import polars as pl
import duckdb
import gc
import psutil, os
import datetime
import json
import requests
import pandas as pd
# pl show more rows
pl.Config.set_tbl_rows(25)


proc = psutil.Process()
proc.memory_info().rss / 1e6        # RSS in MB

today = datetime.date.today().strftime("%Y-%m-%d")
# public s3 scan comparison between duckdb and polars
S3_BASE_URL = "s3://aws-public-blockchain/v1.0/btc"

con = duckdb.connect()

# -----// checking con methods



# scan urls for transactions and blocks
scan_date_prefix="2009-*"  # for testing, scan all of 2026
btc_tx_scan_url = f'{S3_BASE_URL}/transactions/date={scan_date_prefix}/*.parquet'
btc_block_scan_url = f'{S3_BASE_URL}/blocks/date={scan_date_prefix}/*.parquet'


# latest block fetch

def get_block_tip():
    r = requests.get("https://mempool.space/api/blocks/tip/height")
    height = r.json()
    return height


h = get_block_tip()

h
h=1
# pull data for h
q= f"""
select * from read_parquet('{btc_block_scan_url}', hive_partitioning=True) where number == {h}
-- filter by year and pruning 

"""

con.sql(q).pl()

# --- scans on s3 with duckdb 

# ---------------|| Querying BTC transactions from S3 with DuckDB ||--------------


# ----------/// Top N tx by fee per day


query = f"""
with tx as (
select
*
     from read_parquet('{btc_tx_scan_url}', hive_partitioning=True)

    -- where date = '2026-06-04'
)

select block_number, sum(fee) as total_fee, count(*) as tx_count
 from tx
 group by 1
    """

ta = con.sql(query)
ta

query = "Explain " + query


# tx for latest block 
#-----------------------------
query = f"""

select 
    *
from read_parquet('{btc_tx_scan_url}', hive_partitioning=True)

where 
    block_number == {h-1}"""

con.sql(query).pl()




# value counts on inputs and outputs
#====================================
query = f"""

select input_count,output_count, count(*) as total

from read_parquet('{btc_tx_scan_url}', hive_partitioning=True)

where date = '2026-07-07'
group by 1,2
order by 3 desc

"""

df = con.sql(query).pl() 

df




df = pd.DataFrame(
{

"a":[11,21,31,41],
"b":[11,22,33,44]

}

)

df['c'] = df['a'].shift(-1) > df['a'] 

df