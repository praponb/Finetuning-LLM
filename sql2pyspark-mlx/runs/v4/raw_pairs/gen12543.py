from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("service") == "billing")
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.size("context").alias("num_context"),
        F.col("context")["region"].alias("region"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("direct")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("num_context") >= 62, "LARGE").otherwise("SMALL").alias("num_context_tier"),
    )
    .agg(
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("level") == "INFO", 1)), F.lit(0)), 2).alias("duration_sec_per_info"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
    )
)
