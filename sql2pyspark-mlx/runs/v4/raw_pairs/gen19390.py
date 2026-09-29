from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "context",
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)
