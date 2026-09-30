from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.size("context").alias("num_context"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
    )
)
