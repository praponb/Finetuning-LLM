from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .rollup(
        F.when(F.col("latency_ms") >= 2609, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
    )
    .agg(
        F.stddev_samp("status_code").alias("status_code_stddev"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.sum(F.when(F.col("service") == "billing", F.col("status_code")).otherwise(0)).alias("billing_status_code"),
        F.count_if(F.col("service") == "billing").alias("billing_events"),
    )
)
