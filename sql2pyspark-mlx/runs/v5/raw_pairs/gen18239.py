from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("latency_ms") == 1099)
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "payload_json",
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    parsed
    .withColumn("hour_window", F.date_trunc("hour", "log_ts"))
    .withColumn("num_context_tier", F.when(F.col("num_context") >= 82, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["hour_window", "num_context_tier"], ["hour_window"]],
        "hour_window",
        "num_context_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.min("log_ts").alias("started_at"),
    )
    .orderBy(F.col("num_context_tier").desc_nulls_first())
)
