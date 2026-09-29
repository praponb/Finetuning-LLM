from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("latency_ms") >= 2304, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
        "service",
    )
    .agg(
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("key") == "trace", 1)), F.lit(0)), 2).alias("latency_ms_per_trace"),
        F.min("log_ts").alias("first_seen"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
    )
    .orderBy("latency_ms_per_trace")
)
