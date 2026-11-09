"""Run against the Rust server configured with --backend jvm."""

import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.remote(
    os.environ.get("SPARK_REMOTE", "sc://127.0.0.1:15002")
).getOrCreate()
try:
    left = spark.range(5).withColumnRenamed("id", "key")
    right = spark.range(3).withColumnRenamed("id", "key")
    print("JVM DataFrame join:", left.join(right, "key").orderBy("key").collect())
    print(
        "JVM complex types:",
        spark.sql(
            "SELECT CAST('12.34' AS DECIMAL(10,2)) AS amount, array(1,2,3) AS items"
        ).collect(),
    )
finally:
    spark.stop()
