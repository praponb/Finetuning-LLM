from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("latency_ms") >= 2342, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.size(F.collect_set("k")).alias("num_ks"),
        F.sum(F.when(F.col("host") == "h3", F.col("latency_ms")).otherwise(0)).alias("h3_latency_ms"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("service") == "search", 1)), F.lit(0)), 2).alias("latency_ms_per_search"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
    )
    .orderBy("latency_ms_stddev")
)
