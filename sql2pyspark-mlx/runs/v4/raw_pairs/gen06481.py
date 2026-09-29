from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)
