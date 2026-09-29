from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("host") == "h1")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("status_code") >= 388, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
    )
)
