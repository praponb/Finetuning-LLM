from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w2 = Window.orderBy(F.col("status_code").desc(), "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.col("context")["region"].alias("region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    unpacked
    .withColumn("status_code_rank", F.rank().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)
