from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("latency_ms") >= 2225, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
    )
    .agg(
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.count_if(F.col("service") == "auth").alias("auth_events"),
        F.sum(F.when(F.col("service") == "checkout", F.col("latency_ms")).otherwise(0)).alias("checkout_latency_ms"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
    )
    .orderBy(F.col("checkout_latency_ms").desc())
)
