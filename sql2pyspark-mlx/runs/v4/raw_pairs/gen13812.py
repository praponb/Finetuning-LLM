from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("level").isin(["ERROR", "WARN"]))
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("latency_ms") >= 2614, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.percentile_approx("status_code_high", [0.25, 0.5, 0.75]).alias("status_code_high_quartiles"),
        F.count_if(F.col("level") == "WARN").alias("warn_events"),
        F.max("log_ts").alias("ended_at"),
    )
    .orderBy("latency_ms_tier")
)
