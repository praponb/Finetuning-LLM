from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "tier").orderBy("latency_ms", "id")

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    parsed
    .select(
        "id",
        "level",
        "latency_ms",
        "tier",
        F.lead("latency_ms").over(w).alias("next_latency_ms"),
        F.lag("status_code").over(w).alias("prev_status_code"),
        F.lead("status_code").over(w).alias("next_status_code"),
        F.row_number().over(w).alias("seq_no"),
    )
)
