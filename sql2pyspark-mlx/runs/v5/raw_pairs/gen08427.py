from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("service").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_app_logs
    .filter(F.col("service").isin(["search", "auth", "checkout"]))
    .select(
        "id",
        "service",
        "log_ts",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.size("context").alias("num_context"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "service",
        "retry_count",
        "has_region",
        "num_context",
        F.min("retry_count").over(w_part).alias("running_min_retry_count"),
        F.avg("retry_count").over(w_part).alias("running_avg_retry_count"),
        F.max("retry_count").over(w_part).alias("running_max_retry_count"),
    )
)
