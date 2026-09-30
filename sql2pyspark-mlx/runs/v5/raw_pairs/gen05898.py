from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("direct")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)
