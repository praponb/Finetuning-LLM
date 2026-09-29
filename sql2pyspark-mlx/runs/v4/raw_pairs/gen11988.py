from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("retry_count", "id")
w_grp = Window.partitionBy("service", "level").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .filter(F.col("status_code").between(269, 281))
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        "payload_json",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.row_number().over(w_hist).alias("seq_no"),
        F.sum("retry_count").over(w_grp).alias("running_retry_count"),
    )
)
