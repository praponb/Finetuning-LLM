from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w_recent = Window.orderBy("status_code", "id")
w3 = Window.partitionBy("level").orderBy("retry_count", "id")

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context").getItem("region").alias("region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    parsed
    .select(
        "id",
        "duration_text",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.lag("retry_count", 1).over(w_recent).alias("prev_retry_count"),
        F.lag("status_code", 1, 0).over(w3).alias("prev_status_code"),
    )
)
