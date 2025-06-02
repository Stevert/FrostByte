import os
import uuid

from pyspark.sql import SparkSession


def execute_spark(query, storage_path):
    spark = SparkSession.builder.appName("IcebergSpark").getOrCreate()
    df = spark.sql(query)
    path = os.path.join(storage_path, f"result_{uuid.uuid4().hex}.arrow")
    df.write.format("arrow").save(path)
    return [path]
