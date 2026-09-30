from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "service").orderBy(F.col("latency_ms").desc_nulls_first())
w2 = Window.orderBy(F.col("latency_ms").asc_nulls_last())
w3 = Window.orderBy(F.col("latency_ms").desc())
w4 = Window.partitionBy("service", "key")
w5 = Window.partitionBy("key")
w6 = Window.partitionBy("service", "key").orderBy(F.col("latency_ms").desc_nulls_last())
w7 = Window.partitionBy("service")
w8 = Window.partitionBy("key", "service").orderBy(F.col("latency_ms_dense_rank").asc_nulls_first())
w9 = Window.orderBy(F.col("latency_ms_dense_rank_rank").desc_nulls_last())
w10 = Window.orderBy(F.col("latency_ms_dense_rank_rank").desc())
w11 = Window.orderBy("latency_ms_rank_3")

flattened = (
    df_app_logs
    .select("id", "service", "latency_ms", F.explode_outer("context").alias("key", "v"))
)

metrics = (
    flattened
    .select(
        "*",
        F.dense_rank().over(w).alias("latency_ms_dense_rank"),
        F.rank().over(w2).alias("latency_ms_rank"),
        F.percent_rank().over(w3).alias("latency_ms_percent_rank"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.rank().over(w3).alias("latency_ms_rank_2"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4).alias("latency_ms_share"),
        F.max("latency_ms_rank").over(w5).alias("key_max_latency_ms_rank"),
        F.rank().over(w6).alias("latency_ms_rank_3"),
    )
)

scored = (
    enriched
    .withColumn("latency_ms_rank_3_share", F.round((F.col("latency_ms_rank_3") / F.sum("latency_ms_rank_3").over(w7)), 4))
    .withColumn("latency_ms_dense_rank_rank", F.rank().over(w8))
)

scored_app_logs = (
    scored
    .select(
        "*",
        F.min("latency_ms_rank_2").over(w7).alias("service_min_latency_ms_rank_2"),
        F.cume_dist().over(w9).alias("latency_ms_dense_rank_rank_cume_dist"),
    )
)

metrics_app_logs = (
    scored_app_logs
    .withColumn("latency_ms_share_2", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w7)), 4))
    .withColumn("service_avg_latency_ms_dense_rank", F.avg("latency_ms_dense_rank").over(w4))
    .withColumn("latency_ms_dense_rank_rank_percent_rank", F.percent_rank().over(w10))
    .withColumn("latency_ms_dense_rank_rank_share", F.round((F.col("latency_ms_dense_rank_rank") / F.sum("latency_ms_dense_rank_rank").over(w7)), 4))
    .withColumn("latency_ms_rank_3_rank", F.rank().over(w11))
)

result = (
    metrics_app_logs
    .groupBy("service")
    .agg(
        F.percentile_approx("latency_ms_rank_3_rank", [0.25, 0.5, 0.75]).alias("latency_ms_rank_3_rank_quartiles"),
        F.sum(F.when(F.col("key") == "region", F.col("latency_ms_rank_3_share")).otherwise(0)).alias("region_latency_ms_rank_3_share"),
        F.min("latency_ms").alias("min_latency_ms"),
    )
)
