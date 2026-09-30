from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("status_code") >= 424, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_rows"),
        F.sum(F.when(F.col("level") == "DEBUG", F.col("latency_ms")).otherwise(0)).alias("debug_latency_ms"),
        F.max(F.when(F.col("level") == "WARN", F.col("latency_ms"))).alias("warn_max_latency_ms"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
    )
)
