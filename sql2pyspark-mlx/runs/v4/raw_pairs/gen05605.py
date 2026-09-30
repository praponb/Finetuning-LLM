from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context")["trace"].alias("trace"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context")["region"].alias("region"),
    )
)
