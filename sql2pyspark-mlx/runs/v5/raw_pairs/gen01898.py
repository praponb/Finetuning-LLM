from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "payload_json",
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context").getItem("region").alias("region"),
    )
)
