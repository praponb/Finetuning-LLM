from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("log_ts") < "2025-06-01")
    .select(
        "id",
        "log_ts",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)
