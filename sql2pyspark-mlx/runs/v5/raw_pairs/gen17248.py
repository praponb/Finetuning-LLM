from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        F.col("context")["version"].alias("version"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("none")).alias("trace_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
    )
)
