from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "payload_json",
        "context",
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.col("context")["version"].alias("version"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
    )
)
