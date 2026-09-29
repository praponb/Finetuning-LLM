from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("log_ts") < "2024-01-02")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("duration_sec") >= 3.3, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("host") == "h2", F.col("duration_sec")).otherwise(0)).alias("h2_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .filter(F.col("num_rows") > 1)
)
