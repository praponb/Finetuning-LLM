from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "payload_json",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)
