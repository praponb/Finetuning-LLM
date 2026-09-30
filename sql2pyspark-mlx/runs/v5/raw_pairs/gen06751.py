from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("status_code") <= 331)
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context")["version"].alias("version"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
    )
)
