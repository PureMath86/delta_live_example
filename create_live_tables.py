


%pip install dlt

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


inputPath = "/databricks-datasets/structured-streaming/events/"

jsonSchema = StructType([ StructField("time", TimestampType(), True), StructField("action", StringType(), True) ])

def generate_table(live_table):
    @dlt.table(name=live_table, comment="Raw custom data capture for " + live_table)
    def create_live_table():
        json_path = f"{inputPath}{live_table}.json"
        (spark.read.json(json_path))
        
_list = [f"file-{i}" for i in range(50)]

[generate_table(c) for c in _list]
