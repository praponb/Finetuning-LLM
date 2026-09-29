from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
    )
)
