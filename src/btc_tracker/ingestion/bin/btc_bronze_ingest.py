# Exploration on samples catalogue in databricks
from databricks.connect import DatabricksSession
import os
from dotenv import load_dotenv
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import time
from databricks.sdk.runtime import dbutils
load_dotenv()

if os.getenv("DATABRICKS_RUNTIME_VERSION"):  # running ON Databricks
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
else:  # local dev via Databricks Connect
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.remote(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    ).serverless(True).getOrCreate()

# spark.conf.set(
#     "spark.hadoop.fs.s3a.bucket.aws-public-blockchain.aws.credentials.provider",
#     "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
# )

# public s3 scan base url
S3_BASE_URL = "s3://aws-public-blockchain/v1.0/btc"

# pulling sample of tx data from btc

# scan urls for transactions and blocks
scan_date_prefix="2026-06-*"  # for testing, scan all of 2026
btc_tx_scan_url = f'{S3_BASE_URL}/transactions/date={scan_date_prefix}/*.parquet'
btc_block_scan_url = f'{S3_BASE_URL}/blocks/date={scan_date_prefix}/*.parquet'


tx_query = f"""

select * from PARQUET.FILE('{btc_tx_scan_url}') 
where date = '2026-06-04'
limit 100
"""



spark.sql(tx_query).show()

df = spark.read.parquet(f"{S3_BASE_URL}/transactions/")
df = df.where("date = '2026-06-04'")
df.show(100)


# ----- py arrow test for rapid query

s3 = pafs.S3FileSystem(anonymous=True, region="us-east-2")  # check bucket region
dataset = ds.dataset(
    "aws-public-blockchain/v1.0/btc/transactions/",
    filesystem=s3,
    partitioning="hive"
)


spark.sql("""
CREATE TABLE IF NOT EXISTS btc_transactions 
USING PARQUET 
LOCATION 's3://aws-public-blockchain/v1.0/btc/transactions/'
""")


df_filtered.rdd.getNumPartitions()
df = spark.read.parquet(f"{S3_BASE_URL}/transactions/")
df_filtered = df.where("date = '2026-06-04'")

df_filtered.select( "txid", "input_count").show(5)









import boto3
from botocore import UNSIGNED
from botocore.config import Config

s3 = boto3.client("s3", region_name="us-east-2", config=Config(signature_version=UNSIGNED))

resp = s3.list_objects_v2(
    Bucket="aws-public-blockchain",
    Prefix="v1.0/btc/transactions/date=2026-06-04/"
)

files = resp.get("Contents", [])
total_bytes = sum(f["Size"] for f in files)
print(f"{len(files)} files, {total_bytes / 1e6:.1f} MB")