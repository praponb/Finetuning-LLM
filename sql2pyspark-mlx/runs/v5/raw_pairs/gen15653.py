from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        "payload_json",
        "context",
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("status_code") >= 443, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.min("log_ts").alias("first_log_ts"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("status_code_tier")
)
