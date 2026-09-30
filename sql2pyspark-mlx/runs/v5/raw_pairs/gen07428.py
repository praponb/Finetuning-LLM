from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(~F.col("host").isin(["h4", "h3", "h1"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        "payload_json",
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    parsed
    .groupBy(
        "host",
        F.when(F.col("status_code") >= 388, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
        F.when(F.col("duration_sec") >= 3.8, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.sum(F.when(F.col("host") == "h4", F.col("duration_sec")).otherwise(0)).alias("h4_duration_sec"),
        F.sum(F.when(F.col("level") == "DEBUG", F.col("duration_sec")).otherwise(0)).alias("debug_duration_sec"),
        F.stddev_samp("status_code").alias("status_code_stddev"),
    )
    .filter(F.col("h4_duration_sec") > 0)
    .orderBy("h4_duration_sec")
)
