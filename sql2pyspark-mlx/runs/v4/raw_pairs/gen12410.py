from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("status_code") != 377)
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
    )
)

result = (
    parsed
    .cube(F.date_trunc("DAY", "log_ts").alias("activity_day"), "level")
    .agg(
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("level") == "WARN", 1)), F.lit(0)), 2).alias("duration_sec_per_warn"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
    )
    .orderBy(F.col("duration_sec_per_warn").asc_nulls_first())
)
