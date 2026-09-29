from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context").getItem("version"), F.lit("none")).alias("version_or_default"),
        F.col("context")["trace"].alias("trace"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("level") == "WARN").alias("warn_events"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.count_if(F.col("tier") == "free").alias("free_events"),
    )
)
