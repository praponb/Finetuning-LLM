from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context").getItem("trace").alias("trace"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
    )
)

result = (
    parsed
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("num_context") >= 87, "LARGE").otherwise("SMALL").alias("num_context_tier"),
    )
    .agg(
        F.percentile_approx("retry_count", 0.9).alias("retry_count_p90"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.min("log_ts").alias("first_log_ts"),
    )
    .orderBy(F.col("num_context_tier").desc_nulls_last())
)
