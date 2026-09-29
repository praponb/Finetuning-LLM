from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("level").isin(["WARN", "INFO", "DEBUG"]))
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "payload_json",
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)
