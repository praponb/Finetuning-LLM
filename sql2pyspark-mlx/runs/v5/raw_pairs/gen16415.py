from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "status_code",
        "context",
        F.col("context")["version"].alias("version"),
        F.col("context").getItem("region").alias("region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
    )
)
