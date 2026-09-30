from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.to_date("log_ts") < "2024-10-01")
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "payload_json",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("duration_sec") >= 4.4, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.max("log_ts").alias("ended_at"),
    )
    .orderBy(F.col("duration_sec_tier").desc_nulls_first())
)
