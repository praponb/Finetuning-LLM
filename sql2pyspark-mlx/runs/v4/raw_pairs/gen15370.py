from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("duration_sec", "id")
w2 = Window.partitionBy("service").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("service").orderBy("duration_sec", "id")
w4 = Window.partitionBy("host").orderBy("duration_sec", "id")
w5 = Window.partitionBy("service")

unpacked = (
    df_app_logs
    .filter(F.col("service").isin(["search", "checkout", "auth"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "payload_json",
        "context",
        F.col("context")["trace"].alias("trace"),
        F.col("context").getItem("version").alias("version"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.lag("duration_sec", 2).over(w).alias("prev_duration_sec"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_6"),
        F.ntile(3).over(w3).alias("duration_sec_bucket"),
        F.ntile(5).over(w4).alias("duration_sec_bucket_2"),
        F.sum("duration_sec").over(w5).alias("service_sum_duration_sec"),
    )
)
