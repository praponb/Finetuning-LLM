from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "level")
w2 = Window.orderBy("retry_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .filter(F.to_date("log_ts") >= "2024-05-01")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "payload_json",
        F.col("context")["trace"].alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
    )
)

result = (
    flattened_attrs
    .withColumn("host_max_retry_count", F.max("retry_count").over(w))
    .withColumn("cumulative_retry_count", F.sum("retry_count").over(w2))
)
