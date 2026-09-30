from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("level") == "DEBUG")
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context").getItem("region").alias("region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
    )
)
