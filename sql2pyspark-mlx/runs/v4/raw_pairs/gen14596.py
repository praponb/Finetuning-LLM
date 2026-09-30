from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("latency_ms") <= 2248)
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.size("context").alias("num_context"),
    )
)
