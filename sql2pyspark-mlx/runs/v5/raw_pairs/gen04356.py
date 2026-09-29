from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("status_code").between(366, 438))
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    parsed
    .groupBy("tier")
    .agg(
        F.countDistinct("tier").alias("distinct_tiers"),
        F.countDistinct("tier").alias("distinct_tiers_2"),
    )
    .filter(F.col("distinct_tiers_2") >= 1)
)
