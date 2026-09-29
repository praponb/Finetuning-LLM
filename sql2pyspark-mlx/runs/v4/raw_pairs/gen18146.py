from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("host", "service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("k").orderBy("latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy(F.col("latency_ms").desc(), "id", "k")
w5 = Window.orderBy("host_max_latency_ms", "id", "k").rowsBetween(-2, Window.currentRow)
w6 = Window.orderBy("host_max_latency_ms", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("k").orderBy("running_latency_ms", "id", "k").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("host").orderBy(F.col("latency_ms_bucket").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

scored = (
    exploded
    .withColumn("cumulative_latency_ms", F.sum("latency_ms").over(w_running))
    .withColumn("host_max_latency_ms", F.max("latency_ms").over(w2))
    .withColumn("running_latency_ms", F.sum("latency_ms").over(w3))
    .withColumn("latency_ms_bucket", F.ntile(5).over(w_recent))
)

metrics = (
    scored
    .select(
        "*",
        F.avg("host_max_latency_ms").over(w5).alias("host_max_latency_ms_avg_2"),
        F.first("cumulative_latency_ms", ignorenulls=True).over(w6).alias("first_known_cumulative_latency_ms"),
        F.sum("running_latency_ms").over(w6).alias("cumulative_running_latency_ms"),
        F.avg("latency_ms").over(w7).alias("latency_ms_avg_6"),
        F.cume_dist().over(w8).alias("latency_ms_bucket_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "level",
        "latency_ms",
        "k",
        "value",
        "cumulative_latency_ms",
        "running_latency_ms",
        "latency_ms_bucket",
        "host_max_latency_ms_avg_2",
    )
    .orderBy(F.col("running_latency_ms").desc_nulls_first(), "latency_ms_bucket")
)
