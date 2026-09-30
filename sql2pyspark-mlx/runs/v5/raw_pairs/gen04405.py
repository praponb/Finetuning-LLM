from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "payload_json",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context")["region"].alias("region"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    parsed
    .withColumn("activity_day", F.date_trunc("day", "log_ts"))
    .withColumn("status_code_high_tier", F.when(F.col("status_code_high") >= 2, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["service", "activity_day", "status_code_high_tier"], ["service", "activity_day"], ["service"], []],
        "service",
        "activity_day",
        "status_code_high_tier",
    )
    .agg(
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
        F.count_if(F.col("service") == "auth").alias("auth_events"),
    )
    .orderBy(F.col("billing_count").desc())
)
