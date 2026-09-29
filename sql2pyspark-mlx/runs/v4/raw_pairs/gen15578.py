from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 1914, "LARGE").otherwise("SMALL"))
    .withColumn("hour_window", F.date_trunc("hour", "log_ts"))
    .groupingSets(
        [["k", "latency_ms_tier", "hour_window"], ["k", "latency_ms_tier"], ["k"]],
        "k",
        "latency_ms_tier",
        "hour_window",
    )
    .agg(
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.min("log_ts").alias("first_seen"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.min("log_ts").alias("started_at"),
        F.grouping("k").alias("gid"),
    )
    .orderBy(F.col("k").desc())
)
