from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .select(
        "id",
        "host",
        "log_ts",
        "attr_key",
        "value",
        F.count("latency_ms").over(w).alias("running_count_latency_ms"),
        F.max("latency_ms").over(w).alias("running_max_latency_ms"),
        F.sum("latency_ms").over(w).alias("running_sum_latency_ms"),
        F.sum("status_code").over(w2).alias("status_code_sum_1h"),
    )
)
