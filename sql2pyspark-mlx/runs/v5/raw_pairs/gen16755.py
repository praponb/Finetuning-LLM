from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("level").isin(["DEBUG", "INFO"]))
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.col("context").getItem("trace").alias("trace"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)
