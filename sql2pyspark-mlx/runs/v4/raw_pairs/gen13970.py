from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "status_code",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.size("context").alias("num_context"),
    )
)
