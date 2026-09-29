from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.col("context")["trace"].alias("trace"),
        F.coalesce(F.col("context").getItem("version"), F.lit("none")).alias("version_or_default"),
        F.size("context").alias("num_context"),
        F.col("context")["region"].alias("region"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
    )
)

result = (
    flattened_attrs
    .withColumn("num_context_tier", F.when(F.col("num_context") >= 73, "HIGH").otherwise("LOW"))
    .withColumn("hour_window", F.date_trunc("HOUR", "log_ts"))
    .groupingSets(
        [["num_context_tier", "hour_window"], ["num_context_tier"], []],
        "num_context_tier",
        "hour_window",
    )
    .agg(
        F.sum(F.when(F.col("level") == "WARN", F.col("latency_ms"))).alias("warn_total_latency_ms"),
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_rows"),
        F.avg("status_code").alias("avg_status_code"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code_2"),
        F.max("log_ts").alias("last_seen"),
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
    )
    .filter(F.col("avg_status_code") > 0)
    .orderBy(F.col("warn_total_latency_ms").desc())
)
