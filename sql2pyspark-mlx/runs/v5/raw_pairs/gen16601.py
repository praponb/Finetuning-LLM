from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("latency_ms") >= 2610, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.percentile_approx("latency_ms", 0.95).alias("latency_ms_p95"),
        F.sum("status_code").alias("total_status_code"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("service") == "search", 1)), F.lit(0)), 2).alias("latency_ms_per_search"),
        F.max("latency_ms").alias("max_latency_ms"),
    )
    .filter(F.col("latency_ms_per_search") > 0)
    .orderBy(F.col("latency_ms_tier").asc_nulls_last())
)
