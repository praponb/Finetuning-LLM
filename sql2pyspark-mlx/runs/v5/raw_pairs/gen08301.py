from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "context",
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    parsed
    .withColumn("hour_window", F.date_trunc("HOUR", "log_ts"))
    .cube("hour_window")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("level").alias("distinct_levels"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy("distinct_levels")
)
