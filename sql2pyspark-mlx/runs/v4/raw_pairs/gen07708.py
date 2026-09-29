from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "payload_json",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.coalesce(F.col("context").getItem("region"), F.lit("direct")).alias("region_or_default"),
    )
)

result = (
    parsed
    .groupBy(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.when(F.col("is_flagged") >= 1, "HIGH").otherwise("LOW").alias("is_flagged_tier"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_count"),
    )
)
