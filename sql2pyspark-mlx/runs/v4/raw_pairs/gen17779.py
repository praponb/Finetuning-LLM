from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context")["trace"].alias("trace"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.col("context")["region"].alias("region"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.max("log_ts").alias("last_log_ts"),
        F.sum(F.when(F.col("level") == "DEBUG", F.col("retry_count")).otherwise(0)).alias("debug_retry_count"),
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
    )
)
