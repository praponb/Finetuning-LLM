from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.to_date("log_ts") >= "2023-09-01")
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.col("context")["version"].alias("version"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    parsed
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("latency_ms") >= 1924, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.bit_and("status_code").alias("status_code_bit_and"),
    )
    .orderBy("hour_window")
)
