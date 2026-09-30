from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("key").orderBy(F.col("latency_ms").asc_nulls_last())
w3 = Window.partitionBy("key").orderBy(F.col("latency_ms").desc_nulls_last())
w4 = Window.partitionBy("key").orderBy(F.col("latency_ms").desc_nulls_first())
w5 = Window.partitionBy("key")

lines = (
    df_app_logs
    .select("id", "log_ts", "latency_ms", F.explode("context").alias("key", "value"))
)

kept_keys = (
    lines
    .select("id")
    .intersectAll(
        df_app_logs
        .filter(F.col("log_ts") >= "2024-11-01")
        .select("id")
    )
)

verified = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.max("latency_ms").over(w).alias("latency_ms_max_2h"),
        F.dense_rank().over(w2).alias("latency_ms_dense_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("log_ts") > F.to_date(F.lit("2025-06-01")))
    .select(
        "latency_ms",
        "key",
        "latency_ms_max_2h",
        F.hour("log_ts").alias("log_ts_hour"),
    )
)

scored = (
    cleaned
    .withColumn("latency_ms_rank", F.rank().over(w3))
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w4))
    .withColumn("key_sum_latency_ms", F.sum("latency_ms").over(w5))
)

result = (
    scored
    .groupBy("key")
    .agg(
        F.min("latency_ms_rank").alias("min_latency_ms_rank"),
        F.round(F.avg("latency_ms_dense_rank"), 2).alias("avg_latency_ms_dense_rank"),
    )
    .orderBy(F.col("avg_latency_ms_dense_rank").desc_nulls_last())
)
