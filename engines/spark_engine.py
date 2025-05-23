from pyspark.sql import SparkSession
import uuid
import os
from pyspark.sql.functions import col

def execute_spark(query, storage_path):
    spark = SparkSession.builder.appName("IcebergSpark").getOrCreate()
    df = spark.sql(query)
    path = os.path.join(storage_path, f"result_{uuid.uuid4().hex}.arrow")
    df.write.format("arrow").save(path)
    return [path]