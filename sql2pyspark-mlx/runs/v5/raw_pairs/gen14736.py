from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("latency_ms").desc_nulls_last())

unpacked = (
    df_app_logs
    .filter(F.col("latency_ms") >= 1081)
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "context",
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.col("context").getItem("version").alias("version"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.sum("latency_ms").over(w).alias("running_sum_latency_ms"),
        F.avg("latency_ms").over(w).alias("running_avg_latency_ms"),
        F.min("latency_ms").over(w).alias("running_min_latency_ms"),
        F.percent_rank().over(w2).alias("latency_ms_percent_rank"),
    )
)
