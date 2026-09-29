from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        "context",
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)
