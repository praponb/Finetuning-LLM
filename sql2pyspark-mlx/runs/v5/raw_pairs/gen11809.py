from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.to_date("log_ts") >= "2024-10-18")
    .select(
        "id",
        "log_ts",
        F.col("context").getItem("region").alias("region"),
        F.col("context")["version"].alias("version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
    )
)
