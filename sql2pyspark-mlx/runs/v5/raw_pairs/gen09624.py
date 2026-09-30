from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        "payload_json",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context").getItem("trace").alias("trace"),
        F.col("context")["version"].alias("version"),
        F.coalesce(F.col("context").getItem("version"), F.lit("direct")).alias("version_or_default"),
    )
)
