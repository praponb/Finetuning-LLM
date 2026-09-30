from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "payload_json",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    unpacked
    .groupBy(
        F.date_trunc("hour", "log_ts").alias("hour_window"),
        "service",
        F.date_trunc("day", "log_ts").alias("activity_day"),
    )
    .agg(
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_rows"),
        F.sum(F.when(F.col("service") == "auth", F.col("latency_ms")).otherwise(0)).alias("auth_latency_ms"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("service") == "auth", 1)), F.lit(0)), 2).alias("duration_sec_per_auth"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.sum(F.when(F.col("service") == "billing", F.col("duration_sec")).otherwise(0)).alias("billing_duration_sec"),
    )
)
