#Daily cost watchdog: how much Serverless quota burned, by SKU.


from _duckdb import DuckDBPyConnection
from polars.dataframe.frame import DataFrame
from polars.series.series import Series
import os
import delta_sharing
import polars as pl
from databricks import sql
from dotenv import load_dotenv

pl.Config(tbl_rows=-1, fmt_str_lengths=100)
load_dotenv()

conn = sql.connect(
    server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
    http_path=os.getenv(
        "DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/ffd32e8cc089ac87"
    ),
    access_token=os.environ["DATABRICKS_TOKEN"],
)
cur = conn.cursor()

# --- query 1: daily burn by SKU (usage_unit differs per row: never sum across units)
Q_DAILY_BURN = """
SELECT usage_date, sku_name, usage_unit, SUM(usage_quantity) AS total
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE() - INTERVAL 20 DAYS
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC
"""




cur.execute(Q_DAILY_BURN)
df_daily = pl.from_arrow(cur.fetchall_arrow())
print(df_daily)
df_daily
df_daily.write_clipboard()


# --- raw table

q_ = """
SELECT *
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE() - INTERVAL 1 DAYS

"""
cur.execute(q_)
df_raw: DataFrame | Series = pl.from_arrow(cur.fetchall_arrow())
print(df_raw)   
df_raw.show()
# drop specific columns that are not needed for analysis
df_raw = df_raw.drop(["custom_tags","product_features","identity_metadata","usage_metadata"],strict=False)


df_raw.write_clipboard()

# print type of each column
df_raw.schema

# 1. daily burn - what I need
df_raw.group_by(["usage_date","sku_name","usage_unit"]).agg([
  pl.sum("usage_quantity").alias("total"),
  pl.len().alias("n")
]).sort(["usage_date","total"], descending=[True,True]).write_clipboard()



# --- query 2: serverless-only daily (the bucket that blocked us before)
Q_SERVERLESS_DAILY = """
SELECT usage_date, SUM(usage_quantity) AS serverless_dbus
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE() - INTERVAL 60 DAYS
  AND sku_name LIKE '%SERVERLESS%'
GROUP BY 1
ORDER BY 1 DESC
"""

cur.execute(Q_SERVERLESS_DAILY)
df_serverless: DataFrame | Series = pl.from_arrow(cur.fetchall_arrow())
print(df_serverless)

#
cur.close()
conn.close()


# delta sharing 


import delta_sharing


c=delta_sharing.SharingClient(profile='config.share');
print(c.list_all_tables())

url = "config.share#btc-dashboard.btc_data.blocks"
tbl = delta_sharing.load_as_pandas(url)









# import json
# import duckdb

# # 1. Read and parse your local config.share file
# with open("config.share", "r") as f:
#     config = json.load(f)

# endpoint = config["endpoint"]
# token = config["bearerToken"]

# # 2. Initialize DuckDB and load the delta sharing extension
# con = duckdb.connect()
# con.sql("INSTALL duckdb_delta_sharing FROM community;")
# con.sql("LOAD duckdb_delta_sharing;")
# con.sql("INSTALL httpfs;")
# con.sql("LOAD httpfs;")

# # 3. Inject the parsed credentials into a DuckDB Secret
# con.sql(f"""
#     CREATE SECRET (
#         TYPE delta_sharing,
#         PROVIDER config,
#         ENDPOINT '{endpoint}',
#         BEARER_TOKEN '{token}'
#     );
# """)

# # 4. Query your blocks table lazily (pushing down filters, no RAM bloat)
# query = """
#     SELECT * 
#     FROM delta_share_read('btc-dashboard', 'btc_data', 'price_minute')
#      limit 1000
# """

# # Stream results directly into a Polars LazyFrame/DataFrame
# polars_df = con.sql(query).pl()
# print(polars_df)


# polars_df.shape