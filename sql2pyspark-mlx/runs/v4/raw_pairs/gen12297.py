from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        F.coalesce(F.col("context").getItem("version"), F.lit("direct")).alias("version_or_default"),
        F.col("context").getItem("version").alias("version"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)
