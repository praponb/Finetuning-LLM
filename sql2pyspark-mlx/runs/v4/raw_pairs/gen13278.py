from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("latency_ms") < 610)
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.col("context")["region"].alias("region"),
        F.col("context").getItem("version").alias("version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context").getItem("trace").alias("trace"),
    )
)
