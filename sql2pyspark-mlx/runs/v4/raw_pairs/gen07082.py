from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("log_ts_date", "id", "k")
w2 = Window.orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

filtered = (
    flattened
    .filter(F.col("k").isNotNull() & (F.col("log_ts") < "2025-05-05"))
    .withColumn("latency_ms_per_status_code", F.round((F.col("latency_ms") / F.col("status_code")), 2))
    .withColumn("log_ts_date", F.to_date("log_ts"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.lag("latency_ms", 2).over(w).alias("prev_latency_ms"),
        F.sum("latency_ms").over(w2).alias("cumulative_latency_ms"),
    )
)

result = (
    with_windows
    .groupBy("k")
    .agg(
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.sum("latency_ms_per_status_code").alias("total_latency_ms_per_status_code"),
    )
)
