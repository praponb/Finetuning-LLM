from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.col("context")["trace"].alias("trace"),
    )
)
