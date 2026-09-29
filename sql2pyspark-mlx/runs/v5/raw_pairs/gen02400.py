from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("retry_count", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("duration_sec", "id").rowsBetween(-6, -1)

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "service",
        "duration_sec",
        "retry_count",
        F.avg("retry_count").over(w).alias("last3_avg_retry_count"),
        F.max("retry_count").over(w).alias("last3_max_retry_count"),
        F.count("retry_count").over(w).alias("last3_count_retry_count"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_6_prior"),
    )
)
