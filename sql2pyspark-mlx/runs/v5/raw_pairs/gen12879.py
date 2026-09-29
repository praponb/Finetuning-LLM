from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("service") == "billing")
    .select(
        "id",
        "service",
        "level",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)
