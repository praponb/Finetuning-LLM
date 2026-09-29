from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.size("context").alias("num_context"),
    )
)

result = (
    unpacked
    .withColumn("hour_window", F.date_trunc("HOUR", "log_ts"))
    .groupingSets([["level", "hour_window"], ["level"]], "level", "hour_window")
    .agg(
        F.max("log_ts").alias("last_seen"),
        F.sum("num_context").alias("total_num_context"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.grouping_id().alias("grouping_level"),
    )
)
