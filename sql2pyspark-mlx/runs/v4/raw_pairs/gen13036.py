from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("latency_ms") < 2050)
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
    )
)
