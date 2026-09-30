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
        F.when(F.col("status_code") >= 399, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
        "level",
    )
    .agg(
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_rows"),
        F.count("*").alias("event_count"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.count(F.when(F.col("host") == "h4", 1)).alias("h4_count"),
    )
    .filter(F.col("latency_ms_stddev") > 0)
)
