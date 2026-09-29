from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "payload_json",
        "context",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)
