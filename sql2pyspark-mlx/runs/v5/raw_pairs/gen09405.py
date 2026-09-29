from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "payload_json",
        "context",
        F.coalesce(F.col("context").getItem("region"), F.lit("none")).alias("region_or_default"),
        F.col("context")["region"].alias("region"),
    )
)
