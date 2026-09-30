from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("service").orderBy("latency_ms", "id", "key")
w_hist = Window.partitionBy("key", "host").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.partitionBy("host", "key").orderBy("latency_ms", "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    exploded
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "key",
        "v",
        F.sum("latency_ms").over(w).alias("cumulative_latency_ms"),
        F.ntile(5).over(w2).alias("latency_ms_bucket"),
        F.last("key", ignorenulls=True).over(w_hist).alias("last_known_key"),
        F.avg("latency_ms").over(w4).alias("latency_ms_avg_15m"),
        F.lead("latency_ms", 2).over(w5).alias("next_latency_ms"),
    )
)
