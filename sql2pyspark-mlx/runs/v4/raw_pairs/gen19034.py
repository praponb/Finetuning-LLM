from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.coalesce(F.col("context").getItem("version"), F.lit("direct")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.when(F.col("duration_sec") >= 3.4, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("service") == "auth", 1)), F.lit(0)), 2).alias("status_code_per_auth"),
    )
    .orderBy("row_count")
)
