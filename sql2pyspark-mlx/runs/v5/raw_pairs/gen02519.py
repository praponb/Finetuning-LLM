from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "context",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.col("context")["region"].alias("region"),
    )
)

result = (
    flattened_attrs
    .groupBy("service", F.date_trunc("day", "log_ts").alias("activity_day"))
    .agg(
        F.min("log_ts").alias("started_at"),
        F.count("*").alias("num_rows"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("service").desc_nulls_first())
)
