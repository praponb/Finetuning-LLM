from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "level").orderBy(F.col("latency_ms").asc_nulls_last(), "id", "key")
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("host", "level")
w4 = Window.partitionBy("key", "level")
w5 = Window.partitionBy("key")
w6 = Window.partitionBy("key").orderBy(F.col("host_max_latency_ms").desc_nulls_last())
w7 = Window.partitionBy("host").orderBy(F.col("host_max_latency_ms").desc_nulls_first())
w8 = Window.partitionBy("host", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

latest = (
    flattened
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

kept_keys = (
    latest
    .select("host", "id")
    .subtract(
        df_app_logs
        .filter(F.col("log_ts") >= "2025-04-01")
        .select("host", "id")
    )
)

verified = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.max("latency_ms").over(w3).alias("host_max_latency_ms"),
    )
)

metrics = (
    enriched
    .withColumn("latency_ms_share_2", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4))
    .withColumn("host_avg_host_max_latency_ms", F.avg("host_max_latency_ms").over(w2))
)

metrics_app_logs = (
    metrics
    .select(
        "*",
        F.max("host_max_latency_ms").over(w5).alias("key_max_host_max_latency_ms"),
        F.dense_rank().over(w6).alias("host_max_latency_ms_dense_rank"),
    )
)

result = (
    metrics_app_logs
    .withColumn("host_max_latency_ms_rank", F.rank().over(w7))
    .withColumn("host_sum_latency_ms_share_2", F.sum("latency_ms_share_2").over(w8))
)
