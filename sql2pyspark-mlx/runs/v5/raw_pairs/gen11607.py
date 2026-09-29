from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("retry_count").desc(), "id")
w2 = Window.orderBy(F.col("status_code").desc())
w3 = Window.orderBy("latency_ms", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy(F.col("num_context").desc(), "id")

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "payload_json",
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    parsed
    .select(
        "id",
        "service",
        "status_code",
        "has_region",
        "num_context",
        "user_id",
        F.ntile(3).over(w).alias("retry_count_bucket"),
        F.cume_dist().over(w2).alias("status_code_cume_dist"),
        F.avg("latency_ms").over(w3).alias("latency_ms_avg_6"),
        F.ntile(4).over(w4).alias("num_context_bucket"),
    )
)
