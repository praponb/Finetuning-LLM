from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.col("context")["region"].alias("region"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    parsed
    .withColumn("latency_ms_tier", F.when(F.col("latency_ms") >= 2154, "HIGH").otherwise("LOW"))
    .withColumn("hour_window", F.date_trunc("hour", "log_ts"))
    .rollup("latency_ms_tier", "hour_window")
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.countDistinct("service").alias("distinct_services"),
        F.grouping("latency_ms_tier").alias("grouping_id"),
    )
    .filter(F.col("distinct_services") < 1000)
)
