from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("key", "level").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("level")
w3 = Window.orderBy(F.col("latency_ms").asc_nulls_first())
w4 = Window.partitionBy("level").orderBy("latency_ms_cume_dist")
w5 = Window.orderBy(F.col("status_code").desc())
w6 = Window.partitionBy("level").orderBy(F.col("latency_ms_share_2").asc_nulls_last())
w7 = Window.orderBy("status_code")
w8 = Window.partitionBy("key").orderBy(F.col("latency_ms_share_2_percent_rank").asc_nulls_first())
w9 = Window.partitionBy("key").orderBy(F.col("status_code_rank").desc_nulls_last())
w10 = Window.partitionBy("key")
w11 = Window.partitionBy("key").orderBy("status_code_dense_rank")
w12 = Window.partitionBy("key").orderBy(F.col("latency_ms_share_2_percent_rank").asc_nulls_last())
w13 = Window.partitionBy("key", "level").orderBy(F.col("status_code_cume_dist").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

scored = (
    flattened
    .select(
        "*",
        F.rank().over(w_recent).alias("status_code_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.max("latency_ms").over(w2).alias("level_max_latency_ms"),
        F.cume_dist().over(w3).alias("latency_ms_cume_dist"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share_2"),
    )
)

metrics = (
    scored
    .select(
        "level",
        "status_code",
        "key",
        "attr_value",
        "status_code_rank",
        "level_max_latency_ms",
        "latency_ms_share_2",
        F.cume_dist().over(w4).alias("latency_ms_cume_dist_cume_dist"),
        F.dense_rank().over(w5).alias("status_code_dense_rank"),
        F.percent_rank().over(w6).alias("latency_ms_share_2_percent_rank"),
    )
)

scored_app_logs = (
    metrics
    .withColumn("level_stddev_samp_latency_ms_cume_dist_cume_dist", F.stddev_samp("latency_ms_cume_dist_cume_dist").over(w2))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w7))
    .withColumn("latency_ms_share_2_percent_rank_cume_dist", F.cume_dist().over(w8))
)

enriched = (
    scored_app_logs
    .filter(F.col("status_code_dense_rank") != 5)
    .select(
        "*",
        F.max("status_code_dense_rank").over(w2).alias("level_max_status_code_dense_rank"),
        F.rank().over(w9).alias("status_code_rank_rank"),
        F.round((F.col("status_code_dense_rank") / F.sum("status_code_dense_rank").over(w10)), 4).alias("status_code_dense_rank_share"),
    )
)

result = (
    enriched
    .select(
        "level",
        "status_code",
        "key",
        "status_code_dense_rank",
        "latency_ms_share_2_percent_rank",
        "level_stddev_samp_latency_ms_cume_dist_cume_dist",
        "latency_ms_share_2_percent_rank_cume_dist",
        "level_max_status_code_dense_rank",
        F.min("status_code_cume_dist").over(w10).alias("key_min_status_code_cume_dist"),
        F.percent_rank().over(w11).alias("status_code_dense_rank_percent_rank"),
        F.rank().over(w12).alias("latency_ms_share_2_percent_rank_rank"),
        F.percent_rank().over(w13).alias("status_code_cume_dist_percent_rank"),
        F.avg("status_code_cume_dist").over(w10).alias("key_avg_status_code_cume_dist"),
    )
)
