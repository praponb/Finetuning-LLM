from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("duration_sec_tier").orderBy(F.col("avg_status_code").desc())

parsed = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.col("context")["region"].alias("region"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("duration_sec") >= 4.3, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
        F.when(F.col("latency_ms") >= 1982, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.min("status_code").alias("min_status_code"),
    )
    .filter(F.col("avg_status_code") >= 0)
    .withColumn("avg_status_code_rank", F.rank().over(w))
)
