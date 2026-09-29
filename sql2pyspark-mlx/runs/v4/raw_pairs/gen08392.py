from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.col("context")["trace"].alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    unpacked
    .groupBy("level")
    .agg(
        F.min("log_ts").alias("started_at"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.sum(F.when(F.col("level") == "WARN", F.col("duration_sec")).otherwise(0)).alias("warn_duration_sec"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_rows"),
    )
    .orderBy(F.col("warn_duration_sec").desc())
)
