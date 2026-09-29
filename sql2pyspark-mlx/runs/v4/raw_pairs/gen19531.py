from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("latency_ms") >= 2137, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.count("*").alias("event_count"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_rows"),
        F.countDistinct("host").alias("distinct_hosts"),
        F.min("log_ts").alias("first_seen"),
    )
)
