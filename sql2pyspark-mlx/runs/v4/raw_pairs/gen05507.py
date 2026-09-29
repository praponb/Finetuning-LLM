from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "payload_json",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)
