from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("level") == "DEBUG")
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.col("context")["region"].alias("region"),
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
    )
)

result = (
    unpacked
    .groupBy("host", F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.count_if(F.col("host") == "h2").alias("h2_events"),
        F.percentile_approx("num_context", 0.9).alias("num_context_p90"),
        F.max("log_ts").alias("last_log_ts"),
    )
    .orderBy(F.col("hour_window").desc_nulls_last())
)
