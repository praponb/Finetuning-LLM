from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("latency_ms").between(1197, 1658))
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "payload_json",
        F.col("context").getItem("version").alias("version"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["trace"].alias("trace"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    unpacked
    .cube(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.min("log_ts").alias("started_at"),
    )
    .filter(F.col("duration_min") < 1000000)
)
