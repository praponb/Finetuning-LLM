from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("host") == "h3")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("has_fault"),
    )
)
