from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("unknown")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    parsed
    .groupBy("level")
    .agg(
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.sum(F.when(F.col("host") == "h4", F.col("status_code")).otherwise(0)).alias("h4_status_code"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.max("log_ts").alias("ended_at"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
    )
)
