from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w_ordered = Window.partitionBy("level").orderBy("duration_sec", "id")

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.col("context")["region"].alias("region"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.max("status_code").over(w).alias("level_max_status_code"),
        F.lead("status_code", 1).over(w_ordered).alias("next_status_code"),
    )
)
