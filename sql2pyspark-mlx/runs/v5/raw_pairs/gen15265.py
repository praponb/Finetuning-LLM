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
        "context",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)
