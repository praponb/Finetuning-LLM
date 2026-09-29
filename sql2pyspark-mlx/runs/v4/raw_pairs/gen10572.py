from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    lines
    .select(
        "*",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_1h"),
        F.sum("status_code").over(w2).alias("status_code_sum_30m"),
    )
)
