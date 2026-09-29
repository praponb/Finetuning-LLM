from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("duration_sec") >= 4.3, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        "level",
    )
    .agg(
        F.count(F.when(F.col("tier") == "pro", 1)).alias("pro_count"),
        F.min("log_ts").alias("first_seen"),
        F.count_if(F.col("level") == "DEBUG").alias("debug_events"),
        F.percentile_approx("num_context", [0.25, 0.5, 0.75]).alias("num_context_quartiles"),
        F.sum(F.when(F.col("level") == "INFO", F.col("duration_sec")).otherwise(0)).alias("info_duration_sec"),
    )
    .orderBy(F.col("duration_sec_tier").desc_nulls_first())
)
