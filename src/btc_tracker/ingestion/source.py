from databricks.connect import DatabricksSession
import os
from dotenv import load_dotenv
import gc
import marimo as mo
from datetime import datetime, timedelta
import pyarrow.dataset as ds
import pyarrow.fs as pafs
load_dotenv()
gc.collect()

spark = DatabricksSession.builder.remote(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")).serverless(True).getOrCreate()


spark.sql( """
DROP TABLE IF EXISTS workspace.btc_data.blocks_source;""")

spark.sql("""
CREATE TABLE workspace.btc_data.blocks_source
USING PARQUET
OPTIONS (mergeSchema = 'true')
-- PARTITIONED BY (date STRING)
LOCATION 's3://aws-public-blockchain/v1.0/btc/blocks/';""")

spark.sql("""
ALTER TABLE workspace.btc_data.blocks_source RECOVER PARTITIONS;
""")



spark.sql(block_source)
spark.sql("""
DROP TABLE IF EXISTS workspace.btc_data.tx_source
""")

spark.sql("""
CREATE TABLE workspace.btc_data.tx_source
USING PARQUET
OPTIONS (
    mergeSchema = 'true'
)
LOCATION 's3://aws-public-blockchain/v1.0/btc/transactions/'
""")

spark.sql("DESCRIBE TABLE EXTENDED workspace.btc_data.tx_source;").show()






# ------ // health checks on the UC Tables

spark.sql("DESCRIBE DETAIL workspace.btc_data.blocks") \
     .select("partitionColumns", "numFiles", "location") \
     .show(truncate=False)



# ------- sharing tables

query = """GRANT USE CATALOG ON
         CATALOG workspace TO `richardpears46@gmail.com`;
"""

spark.sql(query)

# ----------//arrow

s3 = pafs.S3FileSystem(anonymous=True, region="us-east-2")
dataset = ds.dataset("aws-public-blockchain/v1.0/btc/transactions/",
                     filesystem=s3, partitioning="hive")

dataset.