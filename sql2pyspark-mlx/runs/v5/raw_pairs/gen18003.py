from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        "payload_json",
        F.size("context").alias("num_context"),
        F.col("context")["region"].alias("region"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
    )
)

result = (
    parsed
    .groupBy(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
        F.min("log_ts").alias("first_seen"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.bit_or("status_code").alias("status_code_bit_or_2"),
    )
)
