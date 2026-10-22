# block ingestion into databricks
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
TABLE_NAME = "workspace.btc_data.blocks"
S3_BLOCKS = "s3://aws-public-blockchain/v1.0/btc/blocks/"
LOOKBACK_DAYS = 3
MERGE_KEY = "number"
BLOCKS_SOURCE = "workspace.btc_data.blocks_source" # check source.py for the setup on location and table --- points to public s3
REORG_BUFFER = 10  # 6 is considered standard for reorgs to happen but set to 10 for safety


"""# CALL GRAPH
main()
├── get_spark()
├── get_logfire_token(spark)
├── [backfill]  date_window(3d) → read_blocks_source(tip=0) → merge_blocks(...)     ← overwrites nothing, tops-up 3d/ needs fix
└── [incremental] run_incremental(spark, TABLE_NAME)
    ├── date_window(3d)
    ├── get_local_block_tip(spark, table_name)            [span: "get local block tip"]
    ├── get_public_block_tip(spark, table_name)           [span: "get public s3 block tip"]
    ├── BlockTips(local_tip, source_tip).source_has_new_blocks
    └── if True:
        ├── read_blocks_source(spark, start_date, local_block_tip)   [span: "read_blocks_s3"]
        ├── dedupe_blocks(df)         # row_number over number by last_modified desc
        ├── df.filter(number > local_block_tip) → blocks_to_merge    # new-block check
        └── merge_blocks(spark, df, TABLE_NAME)                      [span: "merging blocks ..."]

    we cannot use spark sql for any of this
"""

#------/// validators

tip_adapter = TypeAdapter(int | None) 

class BlockTips(BaseModel):
    local_tip: int | None = Field(default=None , ge=0)
    source_tip : int | None = Field(default = None , ge=0)

    @property
    def source_has_new_blocks(self) -> bool:
        return(
                self.source_tip is not None 
                and self.local_tip is not None
                and self.source_tip >  self.local_tip
        )



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



#-------///  Dates 
now = datetime.now()
current_date = now.strftime("%Y-%m-%d")
yesterday = (now + timedelta(days=-1)).strftime("%Y-%m-%d")

def read_blocks_s3(spark, start_date: str, end_date: str,latest_block: int = 0, REORG_BUFFER: int = REORG_BUFFER): # swapped to external table/location directly on UC
    """
    Reads blocks from S3 for the given date range and returns a Spark DataFrame.

    Note:
    - the REORG_BUFFER is used to ensure downstream changes to blocks after mining has occured, 
      this assumes that the public s3 bucket conducts reorgs as and when they occur and the blocks are updated in the s3 bucket.

    """

    rel = spark.read.parquet(f"{S3_BLOCKS}")\
        .filter(f"date >= '{start_date}' AND date <= '{end_date}'")\
        .filter(f'number > {latest_block - REORG_BUFFER}')


    return rel

def read_blocks_source(spark,start_date:str, local_block_tip:int = 0 , reorg_buffer:int= REORG_BUFFER):

    s3 = pafs.S3FileSystem(anonymous=True, region="us-east-2")

    dataset = ds.dataset(
        "aws-public-blockchain/v1.0/btc/blocks/",
        filesystem=s3,
        partitioning="hive"
    )


    fragments = dataset.get_fragments(filter=(ds.field("date") >= start_date) )
    paths = sorted({f.path for f in fragments})
    spark_paths = [f"s3://{p.rsplit('/', 1)[0]}/" for p in paths]
    print(len(spark_paths))
    rel = (
        spark.read.option("mergeSchema","true")
        .option("basePath", "s3://aws-public-blockchain/v1.0/btc/blocks")
        .parquet(*spark_paths)
        .filter(f"date >= '{start_date}'")
        .filter(f"number >= {local_block_tip - reorg_buffer}")
           )

    return rel



def merge_blocks(spark, df, table_name: str, merge_key: str = MERGE_KEY):
    """
    Merges the given DataFrame into the specified table using the merge_key.
    """

    delta_table = DeltaTable.forName(spark, table_name)
    delta_table.alias("target").merge(
        df.alias("source"),
        f"target.{merge_key} = source.{merge_key}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()



def get_local_block_tip(spark, table_name: str):
    """
    Returns the latest block number from the core blocks table
    """
    q = f"""
    select max(number) as latest_block_number from {table_name}
    where date >= current_date - 5
    """


    result = spark.sql(q).collect()[0]["latest_block_number"]

    return result or 0

def get_public_block_tip(spark)-> int|None:
    """
    Returns the latest block number from the external blocks table --- public s3
    """

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    # spark_paths = [f"s3://aws-public-blockchain/v1.0/btc/blocks/date={d}/" for d in (yesterday, today,tomorrow)]


    rel = (
    spark.read
    .option("basePath", "s3://aws-public-blockchain/v1.0/btc/blocks")
    .parquet(*spark_paths)

    .select(F.max('number'))
        )

    return rel.collect()[0][0]


def dedupe_blocks(df):
    """
    Keeps the latest version of each block (by last_modified) so the MERGE
    never sees the same `number` twice in a batch.
    """
    w = Window.partitionBy("number").orderBy(F.col("last_modified").desc())
    return (
        df.withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )


@logfire.instrument("Incremental ingestion of blocks into btc_data.blocks")
def run_incremental(spark, table_name: str):
    """
    Runs incremental ingestion of blocks into the specified table.
    """    

    start_date, end_date = date_window(lookback_days=LOOKBACK_DAYS)

    # getting the block height on the main blocks table
    with logfire.span("get local block tip", table_name=table_name):
        local_block_tip = get_local_block_tip(spark, table_name)
        logfire.info(f"local_block_tip - {local_block_tip}",table_name=table_name, local_block_tip=local_block_tip)

    # getting the block height on the block source external table
    with logfire.span("get public s3 block tip",start_date = start_date,
                    end_date = end_date,local_block_tip = local_block_tip):
        df = read_blocks_source(spark, start_date, local_block_tip,reorg_buffer=REORG_BUFFER)
        public_block_tip = df.select(F.max('number')).collect()[0][0]
        logfire.info(f"source_block_tip - {public_block_tip}", table_name = BLOCKS_SOURCE ,source_block_tip = public_block_tip  )
        

    # adding condition for the span to run 
    tips = BlockTips(local_tip= local_block_tip , source_tip= public_block_tip)

    if tips.source_has_new_blocks:
        logfire.info("new_blocks_exist -- ", total_blocks_to_ingest = public_block_tip - local_block_tip)

        with logfire.span("read_blocks_s3", start_date=start_date, end_date=end_date, latest_block=local_block_tip):
            df = read_blocks_source(spark, start_date, local_block_tip,reorg_buffer=REORG_BUFFER)
            # dedupe check on blocks
            df = dedupe_blocks(df)

            # list of blocks in df larger than latest_block
            blocks_to_merge = df.filter(f"number > {local_block_tip}").select("number").distinct().collect()
            blocks_to_merge = [row["number"] for row in blocks_to_merge]
            logfire.info("blocks_to_merge", table_name=table_name, blocks_to_merge=blocks_to_merge)

            if not blocks_to_merge:
                logfire.info("No new blocks to merge")
                return None

        with logfire.span("merging blocks to btc_data.blocks", table_name=table_name, merge_key=MERGE_KEY):
            try:
                merge_blocks(spark,df, table_name, merge_key=MERGE_KEY)
            except Exception as e:
                logfire.error("Error occurred while merging blocks", table_name=table_name, error=str(e))
                raise e
            
        logfire.info("Completed incremental ingestion - added new blocks to table", table_name=table_name, blocks_added=blocks_to_merge)
    else :
        logfire.info("no new blocks to merge")


def backfill():
    """ needs to be added for custom backfills
      given a range for the blocks table"""
    pass



def main():
    mode = os.getenv("INGESTION_MODE", "incremental")  
    spark = get_spark()
    token = get_logfire_token(spark)
    if token:
        os.environ["LOGFIRE_TOKEN"] = token
    logfire.configure(service_name = "btc_data.blocks ingestion service")
    basicConfig(handlers=[logfire.LogfireLoggingHandler()])

    if mode == "backfill":
        start_date, end_date = date_window(lookback_days=LOOKBACK_DAYS)
        logfire.info(f"Backfilling from {start_date} to {end_date}")
        df = read_blocks_source(spark, start_date, local_block_tip=0 , reorg_buffer= REORG_BUFFER)
        merge_blocks(spark, df, TABLE_NAME)

    elif mode == "incremental":
        run_incremental(spark, TABLE_NAME)
    else:
        raise ValueError(f"Unknown INGESTION_MODE: {mode}")




if __name__ == "__main__":
    main()



