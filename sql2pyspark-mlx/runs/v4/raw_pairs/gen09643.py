from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("service").isin(["billing", "auth", "search"]))
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "payload_json",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context")["version"].alias("version"),
    )
)
