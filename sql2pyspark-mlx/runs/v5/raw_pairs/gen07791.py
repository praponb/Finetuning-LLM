from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("service").orderBy("latency_ms", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .filter(F.col("service").isin(["auth", "checkout"]))
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "payload_json",
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "latency_ms",
        F.avg("retry_count").over(w_recent).alias("last3_avg_retry_count"),
        F.count("retry_count").over(w_recent).alias("last3_count_retry_count"),
        F.max("retry_count").over(w_recent).alias("last3_max_retry_count"),
        F.min("retry_count").over(w_recent).alias("last3_min_retry_count"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_2h"),
    )
)
