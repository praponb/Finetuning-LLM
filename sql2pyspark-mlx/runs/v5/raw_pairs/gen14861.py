from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "context",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("flag_bit4") >= 1, "PREMIUM").otherwise("STANDARD").alias("flag_bit4_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("log_ts").alias("first_log_ts"),
    )
)
