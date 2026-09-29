from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.min("log_ts").alias("first_seen"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
    )
)
