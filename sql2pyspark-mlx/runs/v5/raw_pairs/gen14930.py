from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("status_code").between(326, 427))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.col("context").getItem("trace").alias("trace"),
        F.col("context")["version"].alias("version"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

result = (
    parsed
    .withColumn("flag_bit2_tier", F.when(F.col("flag_bit2") >= 1, "LARGE").otherwise("SMALL"))
    .withColumn("hour_window", F.date_trunc("HOUR", "log_ts"))
    .cube("flag_bit2_tier", "hour_window")
    .agg(
        F.sum("status_code").alias("total_status_code"),
        F.count("*").alias("row_count"),
        F.grouping_id().alias("grouping_level"),
    )
)
