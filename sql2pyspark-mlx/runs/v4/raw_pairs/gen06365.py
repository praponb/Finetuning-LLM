from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host").orderBy("status_code", "id", "key")
w3 = Window.orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("host", "key").orderBy(F.col("status_code").desc(), "id", "key")
w5 = Window.partitionBy("key").orderBy(F.col("status_code").desc(), "id", "key")
w6 = Window.partitionBy("host").orderBy("status_code_bucket_2")
w7 = Window.partitionBy("key", "host").orderBy(F.col("status_code_bucket_3").desc())

flattened = (
    df_app_logs
    .select("id", "host", "status_code", F.explode("context").alias("key", "value"))
)

enriched = (
    flattened
    .withColumn("first_known_key", F.first("key", ignorenulls=True).over(w))
    .withColumn("status_code_bucket", F.ntile(4).over(w2))
    .withColumn("cumulative_status_code", F.sum("status_code").over(w3))
    .withColumn("status_code_bucket_2", F.ntile(3).over(w4))
    .withColumn("status_code_bucket_3", F.ntile(10).over(w5))
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    ranked
    .withColumn("dedup_rank_2", F.rank().over(w7))
    .filter(F.col("dedup_rank_2") == 1)
)

result = (
    latest
    .select(
        "id",
        "host",
        "status_code",
        "key",
        "first_known_key",
        "status_code_bucket",
        "cumulative_status_code",
        "status_code_bucket_2",
        "status_code_bucket_3",
    )
    .orderBy(F.col("cumulative_status_code").desc(), "host")
)
