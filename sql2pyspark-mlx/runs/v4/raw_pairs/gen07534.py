from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "payload_json",
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("duration_sec") >= 4.2, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.size(F.collect_set("service")).alias("num_services"),
    )
    .orderBy(F.col("num_services").desc())
)
