from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    flattened
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("status_code") >= 411, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.sum(F.when(F.col("service") == "checkout", F.col("latency_ms"))).alias("checkout_total_latency_ms"),
        F.count_if(F.col("service") == "checkout").alias("checkout_events"),
        F.sum(F.when(F.col("key") == "trace", F.col("latency_ms"))).alias("trace_total_latency_ms"),
    )
    .orderBy(F.col("status_code_tier").desc_nulls_first())
)
