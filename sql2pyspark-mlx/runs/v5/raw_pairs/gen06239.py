from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("status_code") < 356)
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        "payload_json",
        "context",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("is_flagged"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)
