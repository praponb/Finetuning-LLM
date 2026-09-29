from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "context",
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.col("context")["region"].alias("region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)
