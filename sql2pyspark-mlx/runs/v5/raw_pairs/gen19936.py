from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("latency_ms") >= 1968, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
    )
    .agg(
        F.stddev_samp("status_code").alias("status_code_stddev"),
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
    )
    .filter(F.col("status_code_stddev") > 0)
)
