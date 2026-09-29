from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "log_ts",
        "key",
        F.sum("status_code").over(w).alias("cumulative_status_code"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_1h"),
        F.sum("latency_ms").over(w3).alias("latency_ms_sum_15m"),
    )
)
