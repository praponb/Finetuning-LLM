from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "payload_json",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context").getItem("region").alias("region"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)
