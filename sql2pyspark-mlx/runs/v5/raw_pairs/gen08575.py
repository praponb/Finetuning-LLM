from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    lines
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "key",
        F.max("status_code").over(w).alias("status_code_max_15m"),
        F.count("latency_ms").over(w2).alias("latency_ms_count_30m"),
    )
)
