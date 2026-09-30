from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("latency_ms") <= 808)
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)
