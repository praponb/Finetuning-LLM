from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    flattened
    .rollup(
        F.when(F.col("latency_ms") >= 1945, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.min("log_ts").alias("first_seen"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_rows"),
        F.min("log_ts").alias("started_at"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
    )
    .filter(F.col("status_code_bit_or") > 1)
    .orderBy(F.col("status_code_bit_or").asc_nulls_first())
)
