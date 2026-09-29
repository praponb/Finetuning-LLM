from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context").getItem("version").alias("version"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)
