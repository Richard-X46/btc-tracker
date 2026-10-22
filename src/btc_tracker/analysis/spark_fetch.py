# scanning btc tx and blk with databricks connect
from databricks.connect import DatabricksSession
import os 
from dotenv import load_dotenv
import databricks 

load_dotenv() 

spark = DatabricksSession.builder.serverless().profile("<profile-name>").getOrCreate()

conn = sql.connect(server_hostname=os.getenv("DATABRICKS_HOST"), http_path=os.getenv("DATABRICKS_HTTP_PATH"), access_token=os.getenv("DATABRICKS_TOKEN"))

spark = DatabricksSession.builder.remote(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
).serverless(True).getOrCreate()


df = spark.read.table("samples.nyctaxi.trips")

# list tables in nyctaxi
spark.catalog.listTables("samples.nyctaxi")


df.show(5)

