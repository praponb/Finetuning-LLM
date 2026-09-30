from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("service") == "checkout")
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "payload_json",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("unknown")).alias("trace_or_default"),
    )
)

result = (
    unpacked
    .rollup(
        "level",
        F.when(F.col("latency_ms") >= 2256, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
    )
    .agg(
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.percentile_approx("latency_ms", 0.95).alias("latency_ms_p95"),
        F.avg(F.when(F.col("service") == "auth", F.col("latency_ms"))).alias("auth_avg_latency_ms"),
        F.percentile_approx("latency_ms", 0.9).alias("latency_ms_p90"),
    )
)
