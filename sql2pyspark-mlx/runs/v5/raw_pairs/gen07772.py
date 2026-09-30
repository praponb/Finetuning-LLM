from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("duration_sec").desc(), "id")
w_recent = Window.orderBy("log_ts", "id")
w3 = Window.partitionBy("level").orderBy("latency_ms", "id").rowsBetween(-2, Window.currentRow)

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code_high",
        "has_region",
        F.row_number().over(w_grp).alias("seq_no"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.avg("latency_ms").over(w3).alias("latency_ms_avg_2"),
    )
)
