from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id")
w2 = Window.orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("tier").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_app_logs
    .filter(F.to_date("log_ts") >= "2025-10-05")
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.col("context").getItem("version").alias("version"),
    )
)

result = (
    parsed
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("last_known_tier", F.last("tier", ignorenulls=True).over(w2))
    .withColumn("first_known_tier", F.first("tier", ignorenulls=True).over(w3))
)
