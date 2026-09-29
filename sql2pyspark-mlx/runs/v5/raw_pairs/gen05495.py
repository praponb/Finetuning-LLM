from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "payload_json",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("duration_sec") >= 3.5, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .orderBy(F.col("total_duration_sec").desc())
)
