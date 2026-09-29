from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("latency_ms").desc_nulls_last())
w2 = Window.partitionBy("tier")
w3 = Window.partitionBy("host").orderBy("duration_sec", "id")

unpacked = (
    df_app_logs
    .filter(F.col("level").isin(["INFO", "DEBUG"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    unpacked
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w))
    .withColumn("tier_sum_status_code_high", F.sum("status_code_high").over(w2))
    .withColumn("prev_duration_sec", F.lag("duration_sec", 2, 0).over(w3))
)
