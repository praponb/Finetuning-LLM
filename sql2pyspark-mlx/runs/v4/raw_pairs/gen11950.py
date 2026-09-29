from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.to_date("log_ts") >= "2024-08-01")
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.col("context")["version"].alias("version"),
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
    )
)
