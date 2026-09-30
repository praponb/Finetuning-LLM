from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("log_ts") >= "2024-02-01")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)
