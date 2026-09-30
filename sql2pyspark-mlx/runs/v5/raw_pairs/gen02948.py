from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    unpacked
    .groupBy("host", F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.min("log_ts").alias("started_at"),
        F.min("log_ts").alias("first_log_ts"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("host") == "h3", 1)), F.lit(0)), 2).alias("status_code_per_h3"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles_2"),
    )
    .orderBy(F.col("span_minutes").desc())
)
