from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("latency_ms").desc())
w2 = Window.partitionBy("key")
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("service", "key").orderBy(F.col("latency_ms_percent_rank").desc_nulls_last())
w5 = Window.partitionBy("level", "service")
w6 = Window.partitionBy("service")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "v"),
    )
)

enriched = (
    lines
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w))
    .withColumn("key_sum_latency_ms", F.sum("latency_ms").over(w2))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
)

metrics = (
    enriched
    .withColumn("latency_ms_share_2", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w3)), 4))
)

enriched_app_logs = (
    metrics
    .select(
        "service",
        "level",
        "key",
        "latency_ms_percent_rank",
        "key_sum_latency_ms",
        "latency_ms_share",
        "latency_ms_share_2",
        F.cume_dist().over(w4).alias("latency_ms_percent_rank_cume_dist"),
    )
)

result = (
    enriched_app_logs
    .select(
        "service",
        "key",
        "latency_ms_percent_rank",
        "key_sum_latency_ms",
        "latency_ms_share_2",
        "latency_ms_percent_rank_cume_dist",
        F.sum("latency_ms_share").over(w5).alias("level_sum_latency_ms_share"),
        F.count("latency_ms_percent_rank").over(w6).alias("service_count_latency_ms_percent_rank"),
        F.max("latency_ms_share_2").over(w6).alias("service_max_latency_ms_share_2"),
    )
)
