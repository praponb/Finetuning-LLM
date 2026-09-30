from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("k").orderBy(F.col("latency_ms").desc_nulls_first())
w2 = Window.orderBy(F.col("status_code_plus_latency_ms").desc_nulls_first())
w3 = Window.partitionBy("k", "host")
w4 = Window.partitionBy("host")
w5 = Window.partitionBy("k")
w6 = Window.partitionBy("k").orderBy(F.col("status_code_plus_latency_ms").asc_nulls_last())
w7 = Window.partitionBy("host").orderBy(F.col("latency_ms_dense_rank_share").asc_nulls_last())
w_hist = Window.partitionBy("host").orderBy(F.col("latency_ms").desc_nulls_last())
w9 = Window.partitionBy("k").orderBy(F.col("latency_ms_dense_rank_share_dense_rank").desc_nulls_first())
w10 = Window.partitionBy("host").orderBy(F.col("latency_ms_dense_rank_share_dense_rank").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

valid = (
    flattened
    .filter(F.col("latency_ms") < 1782)
    .select(
        "id",
        "host",
        "latency_ms",
        "k",
        (F.col("status_code") + F.col("latency_ms")).alias("status_code_plus_latency_ms"),
        F.coalesce("k", F.lit("N/A")).alias("k_filled"),
    )
)

base = (
    valid
    .filter(F.col("k") == "version")
    .select(
        "*",
        F.col("latency_ms").cast("string").alias("latency_ms_str"),
        F.coalesce("k", F.lit("UNKNOWN")).alias("k_filled_2"),
    )
)

scored = (
    base
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w_recent))
    .withColumn("status_code_plus_latency_ms_rank", F.rank().over(w2))
    .withColumn("k_avg_status_code_plus_latency_ms", F.avg("status_code_plus_latency_ms").over(w3))
    .withColumn("host_min_status_code_plus_latency_ms", F.min("status_code_plus_latency_ms").over(w4))
    .withColumn("k_stddev_samp_latency_ms", F.stddev_samp("latency_ms").over(w5))
)

with_windows = (
    scored
    .select(
        "*",
        F.round((F.col("latency_ms_dense_rank") / F.sum("latency_ms_dense_rank").over(w3)), 4).alias("latency_ms_dense_rank_share"),
        F.percent_rank().over(w6).alias("status_code_plus_latency_ms_percent_rank"),
    )
)

scored_app_logs = (
    with_windows
    .withColumn("latency_ms_dense_rank_share_dense_rank", F.dense_rank().over(w7))
    .withColumn("latency_ms_rank", F.rank().over(w_hist))
)

metrics = (
    scored_app_logs
    .select(
        "*",
        F.cume_dist().over(w9).alias("latency_ms_dense_rank_share_dense_rank_cume_dist"),
        F.rank().over(w10).alias("latency_ms_dense_rank_share_dense_rank_rank"),
        F.round((F.col("latency_ms_dense_rank") / F.sum("latency_ms_dense_rank").over(w4)), 4).alias("latency_ms_dense_rank_share_2"),
    )
)

result = (
    metrics
    .groupBy("k")
    .agg(
        F.round(F.avg("status_code_plus_latency_ms_rank"), 2).alias("avg_status_code_plus_latency_ms_rank"),
        F.count_if(F.col("host") == "h4").alias("h4_events"),
        F.max("host_min_status_code_plus_latency_ms").alias("max_host_min_status_code_plus_latency_ms"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
)
