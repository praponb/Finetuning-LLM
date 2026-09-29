from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("host").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy("latency_ms_share", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key").orderBy(F.col("latency_ms").desc(), "id", "key")
w5 = Window.partitionBy("key", "service").orderBy("latency_ms", "id", "key").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("host").orderBy("latency_ms", "id", "key").rowsBetween(-6, -1)
w7 = Window.orderBy("latency_ms", "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
        F.first("key", ignorenulls=True).over(w2).alias("first_known_key"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.last("latency_ms", ignorenulls=True).over(w3).alias("last_known_latency_ms"),
        F.ntile(5).over(w4).alias("latency_ms_bucket"),
        F.avg("latency_ms").over(w5).alias("latency_ms_avg_6"),
        F.avg("latency_ms").over(w6).alias("latency_ms_avg_6_prior"),
        F.ntile(4).over(w7).alias("latency_ms_bucket_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "key",
        "value",
        "latency_ms_share",
        "first_known_key",
        "latency_ms_bucket",
    )
)
