from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("status_code") >= 427, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.max(F.when(F.col("k") == "trace", F.col("status_code"))).alias("trace_max_status_code"),
        F.count("*").alias("event_count"),
        F.avg(F.when(F.col("k") == "region", F.col("status_code"))).alias("region_avg_status_code"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("region_avg_status_code") > 0)
)
