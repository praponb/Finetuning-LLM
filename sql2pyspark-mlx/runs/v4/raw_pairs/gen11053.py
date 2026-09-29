from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("status_code").between(283, 356))
    .select(
        "id",
        "level",
        "status_code",
        F.col("context")["version"].alias("version"),
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)
