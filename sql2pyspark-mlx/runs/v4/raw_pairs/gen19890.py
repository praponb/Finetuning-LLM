from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id")
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w3 = Window.partitionBy("duration_sec_tier").orderBy(F.col("avg_duration_sec_bucket").desc())

enriched = (
    df_web_events
    .select(
        "*",
        F.lag("duration_sec", 1).over(w).alias("prev_duration_sec"),
        F.ntile(5).over(w2).alias("duration_sec_bucket"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("duration_sec") >= 395, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        "page",
    )
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.max("duration_sec_bucket").alias("max_duration_sec_bucket"),
        F.sum(F.when(F.col("page") == "/checkout", F.col("duration_sec"))).alias("checkout_total_duration_sec"),
        F.round(F.avg("duration_sec_bucket"), 2).alias("avg_duration_sec_bucket"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"),
        F.avg("duration_sec_bucket").alias("avg_duration_sec_bucket_2"),
    )
    .withColumn("avg_duration_sec_bucket_dense_rank", F.dense_rank().over(w3))
    .orderBy(F.col("checkout_total_duration_sec").desc_nulls_last())
)
