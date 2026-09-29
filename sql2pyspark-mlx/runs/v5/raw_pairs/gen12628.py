from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("host").isin(["h2", "h1", "h4"]))
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "payload_json",
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("latency_ms") >= 2439, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
        "level",
    )
    .agg(
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_rows"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.percentile_approx("num_context", 0.9).alias("num_context_p90"),
        F.max(F.when(F.col("level") == "DEBUG", F.col("num_context"))).alias("debug_max_num_context"),
        F.sum(F.when(F.col("level") == "INFO", F.col("latency_ms")).otherwise(0)).alias("info_latency_ms"),
    )
)
