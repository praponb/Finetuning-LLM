from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("latency_ms") >= 2525, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
        F.sum(F.when(F.col("key") == "version", F.col("latency_ms")).otherwise(0)).alias("version_latency_ms"),
        F.count(F.when(F.col("host") == "h2", 1)).alias("h2_rows"),
    )
    .filter(F.col("version_latency_ms") > 0)
)
