from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_grp = Window.partitionBy("service").orderBy("latency_ms")
w3 = Window.partitionBy("service").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .filter(F.col("service") == "checkout")
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        "payload_json",
        F.coalesce(F.col("context").getItem("trace"), F.lit("none")).alias("trace_or_default"),
        F.col("context")["trace"].alias("trace"),
        F.size("context").alias("num_context"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "latency_ms",
        "status_code",
        "has_trace",
        F.sum("latency_ms").over(w).alias("latency_ms_sum_1h"),
        F.percent_rank().over(w_grp).alias("latency_ms_percent_rank"),
        F.last("latency_ms", ignorenulls=True).over(w3).alias("last_known_latency_ms"),
    )
)
