from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "attr_key").orderBy("status_code", "id", "attr_key")
w2 = Window.partitionBy("level", "host").orderBy(F.col("status_code").desc(), "id", "attr_key")
w3 = Window.orderBy(F.col("status_code").desc(), "id", "attr_key")
w4 = Window.partitionBy("host").orderBy("status_code_bucket", "id", "attr_key").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("level").orderBy("status_code_bucket_2", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("status_code_bucket_2").desc_nulls_first())
w7 = Window.partitionBy("host").orderBy("status_code", "id", "attr_key")
w_part = Window.partitionBy("level").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("level", "attr_key").orderBy("status_code_bucket", "id", "attr_key").rowsBetween(-5, Window.currentRow)
w10 = Window.partitionBy("host")
w11 = Window.partitionBy("attr_key", "service").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("attr_key").orderBy("status_code_bucket_2_avg_2_avg_5", "id", "attr_key")
w13 = Window.partitionBy("attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

enriched = (
    exploded
    .withColumn("prev_status_code", F.lag("status_code", 1).over(w))
    .withColumn("status_code_bucket", F.ntile(5).over(w2))
    .withColumn("status_code_bucket_2", F.ntile(3).over(w3))
)

metrics = (
    enriched
    .select(
        "*",
        F.avg("status_code_bucket_2").over(w4).alias("status_code_bucket_2_avg_2"),
        F.sum("status_code_bucket_2").over(w5).alias("running_status_code_bucket_2"),
        F.percent_rank().over(w6).alias("status_code_bucket_2_percent_rank"),
        F.row_number().over(w7).alias("seq_no"),
    )
)

with_windows = (
    metrics
    .withColumn("cumulative_status_code_bucket_2_percent_rank", F.sum("status_code_bucket_2_percent_rank").over(w_part))
    .withColumn("status_code_bucket_2_avg_2_avg_5", F.avg("status_code_bucket_2_avg_2").over(w9))
    .withColumn("status_code_bucket_2_avg_2_share", F.round((F.col("status_code_bucket_2_avg_2") / F.sum("status_code_bucket_2_avg_2").over(w10)), 4))
)

result = (
    with_windows
    .withColumn("running_status_code_bucket_2_percent_rank", F.sum("status_code_bucket_2_percent_rank").over(w11))
    .withColumn("status_code_bucket_2_avg_2_avg_5_bucket", F.ntile(10).over(w12))
    .withColumn("attr_key_count_status_code", F.count("status_code").over(w13))
)
