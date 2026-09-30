from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("host") == "h1")
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "payload_json",
        "context",
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)
