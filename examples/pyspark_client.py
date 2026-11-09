"""Run against the local DataFusion server using a PySpark Connect client."""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sql_sum

remote = os.environ.get("SPARK_REMOTE", "sc://127.0.0.1:15002")
spark = SparkSession.builder.remote(remote).getOrCreate()
try:
    totals = spark.range(1000).agg(sql_sum(col("id")).alias("total")).collect()
    assert totals[0]["total"] == 499500, totals
    answer = spark.sql("SELECT 1 + 1 AS answer").collect()
    assert answer[0]["answer"] == 2, answer
    print(totals)
    print(answer)
finally:
    spark.stop()
