from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("service") != "checkout")
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "context",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.col("context").getItem("version").alias("version"),
        F.size("context").alias("num_context"),
    )
)
