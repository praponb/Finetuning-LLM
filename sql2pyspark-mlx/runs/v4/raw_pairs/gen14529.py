from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("latency_ms") != 1448)
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)
