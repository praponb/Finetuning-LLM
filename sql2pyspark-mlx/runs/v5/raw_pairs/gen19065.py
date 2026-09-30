from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("service").isin(["auth", "search"]))
    .select(
        "id",
        "service",
        "latency_ms",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["trace"].alias("trace"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
    )
)
