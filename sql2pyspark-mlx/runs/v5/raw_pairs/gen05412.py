from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tier").orderBy(F.col("status_code_high").desc(), "id")
w2 = Window.partitionBy("tier")

unpacked = (
    df_app_logs
    .filter(F.col("host").isin(["h2", "h1", "h4"]))
    .select(
        "id",
        "host",
        "payload_json",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "status_code_high",
        "user_id",
        F.ntile(10).over(w).alias("status_code_high_bucket"),
        F.max("status_code_high").over(w2).alias("tier_max_status_code_high"),
    )
)
