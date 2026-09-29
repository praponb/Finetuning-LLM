from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context")["region"].alias("region"),
    )
)
