from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("latency_ms") >= 2351, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
    )
    .agg(
        F.sum(F.when(F.col("k") == "trace", F.col("latency_ms"))).alias("trace_total_latency_ms"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("k") == "version", 1)), F.lit(0)), 2).alias("latency_ms_per_version"),
        F.percentile_approx("latency_ms", 0.9).alias("latency_ms_p90"),
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_count"),
    )
)
