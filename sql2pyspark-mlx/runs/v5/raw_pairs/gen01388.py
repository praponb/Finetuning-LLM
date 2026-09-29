from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("log_ts") < "2025-10-01")
    .select(
        "id",
        "service",
        "log_ts",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
    )
)

result = (
    unpacked
    .groupBy("service")
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.max("log_ts").alias("ended_at"),
    )
    .filter(F.col("event_count") > 1)
)
