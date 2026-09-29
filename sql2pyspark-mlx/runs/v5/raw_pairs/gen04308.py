from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("log_ts") >= "2025-05-01")
    .select(
        "id",
        "log_ts",
        "payload_json",
        "context",
        F.col("context")["region"].alias("region"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
    )
)

result = (
    parsed
    .groupBy(
        F.date_trunc("day", "log_ts").alias("activity_day"),
        "tier",
        F.date_trunc("hour", "log_ts").alias("hour_window"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("tier") == "free", 1)).alias("free_count"),
        F.count_if(F.col("tier") == "gold").alias("gold_events"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
    )
    .filter(F.col("free_count") >= 1)
    .orderBy("activity_day")
)
