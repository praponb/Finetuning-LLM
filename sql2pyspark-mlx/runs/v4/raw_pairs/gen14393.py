from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)
