from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("level") == "WARN")
    .select(
        "id",
        "host",
        "level",
        "payload_json",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)
