from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("host") == "h4")
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "payload_json",
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context")["version"].alias("version"),
    )
)
