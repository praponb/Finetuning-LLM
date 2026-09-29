from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "context",
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.min("log_ts").alias("first_log_ts"),
        F.min("latency_ms").alias("min_latency_ms"),
        F.count("*").alias("num_rows"),
        F.min("log_ts").alias("first_seen"),
    )
)
