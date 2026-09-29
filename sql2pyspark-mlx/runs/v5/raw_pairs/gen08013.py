from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "service", "host", "latency_ms", F.explode("context").alias("k", "v"))
)

result = (
    lines
    .groupBy(
        "service",
        F.when(F.col("latency_ms") >= 2163, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.percentile_approx("latency_ms", 0.9).alias("latency_ms_p90"),
        F.sum(F.when(F.col("service") == "billing", F.col("latency_ms")).otherwise(0)).alias("billing_latency_ms"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.avg("latency_ms").alias("avg_latency_ms"),
    )
    .filter(F.col("avg_latency_ms") > 0)
    .orderBy(F.col("billing_latency_ms").desc())
)
