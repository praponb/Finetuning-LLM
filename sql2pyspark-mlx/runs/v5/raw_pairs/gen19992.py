from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("log_ts") < "2025-06-29")
    .select(
        "id",
        "log_ts",
        "payload_json",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
    )
)
