from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context").getItem("region").alias("region"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share_2"),
    )
)
