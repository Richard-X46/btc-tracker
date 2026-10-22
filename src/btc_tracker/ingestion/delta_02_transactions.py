# tx ingestion into databricks 
# nested inputs and outputs dropped
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from logging import basicConfig
import logfire
from pyspark.sql import functions as F,Window
from pydantic import BaseModel, Field, TypeAdapter
import pyarrow.dataset as ds
import pyarrow.fs as pafs

load_dotenv()

# =============// CONSTANTS //===============
TX_SOURCE = "workspace.btc_data.tx_source" # registered external table
TABLE_NAME = "workspace.btc_data.transactions"# main
MERGE_KEY = "txid"
LOOKBACK_DAYS = 1
HORIZON= 30
REORG_BUFFER = 10
KEEP_COLS = ["txid","version","size","block_number","index","virtual_size",
             "lock_time","input_count","output_count","is_coinbase",
             "output_value","fee","input_value","block_timestamp","date","last_modified"]


"""CALL GRAPH

main()
├── get_spark()
├── get_logfire_token(spark)
├── date_window(HORIZON)
│
├── [backfill] run_backfill(start_date, end_date, spark, TABLE_NAME)
│   ├── read_tx_source_horizon(spark, start_date)
│   └── df.write.format("delta").mode("overwrite").saveAsTable(table_name)
│
└── [incremental] run_incremental(spark)
    ├── date_window(LOOKBACK_DAYS)
    ├── get_local_tx_tip(spark, TABLE_NAME)
    ├── read_transactions_source(spark, start_date, local_tip)
    │   ├── pyarrow: list S3 partitions by date
    │   └── spark.read.parquet(*paths).filter(block_number >= local_tip - reorg_buffer)
    ├── df.select(max(block_number)) → source_tip
    └── if source_tip > local_tip:
        └── merge_tx(spark, df, TABLE_NAME)

"""

def get_logfire_token(spark):
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):  # on Databricks -> pull from secret
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        return dbutils.secrets.get(scope="btc-tracker", key="logfire-token")
    else:  # local dev -> from .env
        return os.getenv("LOGFIRE_TOKEN")




def get_spark():
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):  # running ON Databricks
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    else:  # local dev via Databricks Connect
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.remote(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")
        ).serverless(True).getOrCreate()
    return spark

def date_window(lookback_days: int = LOOKBACK_DAYS):
    """
    Returns a tuple of (start_date, end_date) for the given lookback_days.
    """
    now = datetime.now()
    end_date = now.strftime("%Y-%m-%d")
    start_date = (now + timedelta(days=-lookback_days)).strftime("%Y-%m-%d")
    return start_date, end_date

# -------// validations

class TxTips(BaseModel):
    local_tip: int | None = Field(default=None, ge=0)
    source_tip: int | None = Field(default=None, ge=0)

    @property
    def source_has_new_blocks(self) -> bool:
        return (self.source_tip is not None
                and self.local_tip is not None
                and self.source_tip > self.local_tip)





# ---------- //Tips
def get_local_tx_tip(spark, table_name):
    r = spark.sql(f"""select 
                            max(block_number) tip 
                        from 
                            {table_name} 
                            where date >= current_date - 5
                        """).collect()[0]["tip"]
    return r or 0



def read_tx_source_horizon(spark, start_date, local_block_tip=0, reorg_buffer=REORG_BUFFER):
    return (spark.read.option("mergeSchema", "true")
            .table(TX_SOURCE)
            .filter(f"date >= '{start_date}' ")
            .filter(f"block_number >= {local_block_tip - reorg_buffer}")
            .select([*KEEP_COLS]))

def read_transactions_source(spark,start_date:str, local_block_tip:int = 0 , reorg_buffer:int= REORG_BUFFER):

    s3 = pafs.S3FileSystem(anonymous=True, region="us-east-2")

    dataset = ds.dataset(
        "aws-public-blockchain/v1.0/btc/transactions/",
        filesystem=s3,
        partitioning="hive"
    )


    fragments = dataset.get_fragments(filter=(ds.field("date") >= start_date) )
    paths = sorted({f.path for f in fragments})
    spark_paths = [f"s3://{p.rsplit('/', 1)[0]}/" for p in paths]
    rel = (
        spark.read.option("mergeSchema","true")
        .option("basePath", "s3://aws-public-blockchain/v1.0/btc/transactions")
        .parquet(*spark_paths)
        .filter(f"date >= '{start_date}'")
        .filter(f"block_number >= {local_block_tip - reorg_buffer}")
        .select([*KEEP_COLS]))
           

    return rel

def run_backfill(start_date,end_date,spark, table_name):
    # Read from tx_source with horizon window
    df = read_tx_source_horizon(spark, start_date)
    # Write as Delta table, partitioned by date
    df.write.format("delta").mode("overwrite").partitionBy("date").saveAsTable(table_name)
    return None

def merge_tx(spark, df, table_name: str, merge_key: str = MERGE_KEY):
    """
    Merges the given DataFrame into the specified table using the merge_key.
    """

    delta_table = DeltaTable.forName(spark, table_name)
    delta_table.alias("target").merge(
        df.alias("source"),
        f"target.{merge_key} = source.{merge_key}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()



@logfire.instrument("Incremental ingestion of tx into btc_data.transactions")
def run_incremental(spark):
    """
    Runs incremental ingestion of tx into the specified table.
    """  

    start_date, end_date = date_window(lookback_days=LOOKBACK_DAYS)

    # local tip
    with logfire.span("get local tip", table_name= TABLE_NAME):
        local_tip = get_local_tx_tip(spark,table_name=TABLE_NAME)
        logfire.info(f"local_block_tip - {local_tip}",table_name=TABLE_NAME, local_tx_tip=local_tip)

    df=read_transactions_source(spark,start_date=start_date,local_block_tip=local_tip) 

    # source tip
    with logfire.span("get source tip", table_name= TABLE_NAME):
        source_tip = df.select(F.max('block_number').alias('source_tip')).collect()[0][0]
        logfire.info(f"source tx tip - {source_tip}", table_name = TABLE_NAME ,source_block_tip = source_tip  )

    if source_tip>local_tip:
        try:
            merge_tx(spark,df,table_name=TABLE_NAME,merge_key=MERGE_KEY)
            logfire.info(f"merged new txid's into btc_data.transactions - total blocks {source_tip-local_tip}")
        except Exception as e:
            logfire.error("Error occurred while merging tx", table_name=TABLE_NAME, error=str(e))
            raise e

    else:
        logfire.info("no new txid's to merge")


def main():
    mode = os.getenv("INGESTION_MODE", "incremental")  
    spark = get_spark()
    token = get_logfire_token(spark)
    if token:
        os.environ["LOGFIRE_TOKEN"] = token
    logfire.configure(service_name = "btc_data.transactions ingestion service")
    basicConfig(handlers=[logfire.LogfireLoggingHandler()])
    


    if mode== 'backfill':
        start_date , end_date = date_window(HORIZON)
        run_backfill(start_date=start_date,end_date=end_date,spark = spark,table_name = TABLE_NAME)

    if mode == 'incremental':
        run_incremental(spark=spark)
        



if __name__ == "__main__":
    main()
