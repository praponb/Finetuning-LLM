from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("status_code", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("level", "tier")

parsed = (
    df_app_logs
    .filter(F.col("level") == "INFO")
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    parsed
    .select(
        "id",
        "host",
        "level",
        "status_code",
        "tier",
        F.sum("status_code").over(w).alias("last4_sum_status_code"),
        F.count("status_code").over(w).alias("last4_count_status_code"),
        F.min("status_code").over(w).alias("last4_min_status_code"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
    )
)
