from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.coalesce(F.col("context").getItem("region"), F.lit("unknown")).alias("region_or_default"),
        F.col("context")["version"].alias("version"),
        F.col("context").getItem("trace").alias("trace"),
    )
)
