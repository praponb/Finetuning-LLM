from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "payload_json",
        F.col("context")["region"].alias("region"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.size("context").alias("num_context"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("host") == "h3", 1)), F.lit(0)), 2).alias("latency_ms_per_h3"),
        F.sum("num_context").alias("total_num_context"),
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_count"),
    )
    .orderBy(F.col("h3_count").desc())
)
