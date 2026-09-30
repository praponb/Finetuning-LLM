from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "payload_json",
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.col("context")["version"].alias("version"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("latency_ms") >= 2664, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.avg(F.when(F.col("host") == "h3", F.col("status_code"))).alias("h3_avg_status_code"),
        F.avg("latency_ms").alias("avg_latency_ms"),
        F.stddev_samp("latency_ms").alias("latency_ms_stddev"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_rows"),
        F.max(F.when(F.col("host") == "h2", F.col("latency_ms"))).alias("h2_max_latency_ms"),
        F.percentile_approx("latency_ms", 0.9).alias("latency_ms_p90"),
    )
)
