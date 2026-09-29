from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "context",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)
