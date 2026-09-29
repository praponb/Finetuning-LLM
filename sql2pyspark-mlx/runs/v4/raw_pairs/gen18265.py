from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("latency_ms").between(2017, 2205))
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "payload_json",
        "context",
        F.coalesce(F.col("context").getItem("version"), F.lit("direct")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context")["region"].alias("region"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    parsed
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.min("log_ts").alias("first_log_ts"),
    )
    .orderBy(F.col("hour_window").desc())
)
