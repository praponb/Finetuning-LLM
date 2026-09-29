from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("host").isin(["h2", "h1", "h3"]))
    .select(
        "id",
        "host",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context").getItem("region"), F.lit("none")).alias("region_or_default"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
    )
)
