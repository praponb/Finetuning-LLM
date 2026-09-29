from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "payload_json",
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.col("context")["region"].alias("region"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    unpacked
    .groupBy(
        "service",
        F.when(F.col("latency_ms") >= 1907, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
    )
    .agg(
        F.sum(F.when(F.col("service") == "checkout", F.col("duration_sec"))).alias("checkout_total_duration_sec"),
        F.size(F.collect_set("service")).alias("num_services"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_count"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
    )
    .orderBy(F.col("avg_latency_ms").desc())
)
