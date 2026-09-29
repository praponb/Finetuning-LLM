from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .rollup(
        F.when(F.col("latency_ms") >= 2571, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.percentile_approx("latency_ms", 0.9).alias("latency_ms_p90"),
        F.sum(F.when(F.col("service") == "checkout", F.col("latency_ms")).otherwise(0)).alias("checkout_latency_ms"),
        F.avg(F.when(F.col("attr_key") == "trace", F.col("status_code"))).alias("trace_avg_status_code"),
        F.avg(F.when(F.col("service") == "billing", F.col("status_code"))).alias("billing_avg_status_code"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.count("*").alias("cnt"),
    )
)
