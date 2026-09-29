from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .groupBy(
        "level",
        "k",
        F.when(F.col("latency_ms") >= 2237, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.sum(F.when(F.col("k") == "version", F.col("latency_ms")).otherwise(0)).alias("version_latency_ms"),
        F.percentile_approx("latency_ms", 0.9).alias("latency_ms_p90"),
        F.min("log_ts").alias("first_log_ts"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
    )
)
