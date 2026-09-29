from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
    )
)

result = (
    flattened_attrs
    .cube(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
    )
    .agg(
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        F.bit_and("status_code").alias("status_code_bit_and"),
    )
    .filter(F.col("status_code_bit_and") > 1)
    .orderBy(F.col("hour_window").desc_nulls_last())
)
