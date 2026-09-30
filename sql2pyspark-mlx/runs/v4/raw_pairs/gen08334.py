from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host")
w2 = Window.partitionBy("k")
w3 = Window.partitionBy("k", "host").orderBy(F.col("latency_ms").desc())
w_ordered = Window.orderBy(F.col("host_max_latency_ms").desc_nulls_first())
w5 = Window.partitionBy("k_count_latency_ms_tier").orderBy(F.col("distinct_hosts").desc())

lines = (
    df_app_logs
    .select("id", "host", "latency_ms", F.explode_outer("context").alias("k", "value"))
)

metrics = (
    lines
    .withColumn("host_max_latency_ms", F.max("latency_ms").over(w))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("latency_ms_share_2", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
    .withColumn("k_count_latency_ms", F.count("latency_ms").over(w2))
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w3))
)

metrics_app_logs = (
    metrics
    .select(
        "*",
        F.rank().over(w_ordered).alias("host_max_latency_ms_rank"),
        F.rank().over(w_ordered).alias("host_max_latency_ms_rank_2"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share_3"),
    )
)

summary = (
    metrics_app_logs
    .groupBy(
        F.when(F.col("k_count_latency_ms") >= 18, "PREMIUM").otherwise("STANDARD").alias("k_count_latency_ms_tier"),
        "host",
        "k",
    )
    .agg(
        F.percentile_approx("host_max_latency_ms_rank_2", 0.9).alias("host_max_latency_ms_rank_2_p90"),
        F.sum(F.when(F.col("host") == "h2", F.col("host_max_latency_ms")).otherwise(0)).alias("h2_host_max_latency_ms"),
        F.countDistinct("host").alias("distinct_hosts"),
        F.percentile_approx("host_max_latency_ms_rank_2", 0.5).alias("host_max_latency_ms_rank_2_p50"),
    )
    .filter(F.col("host_max_latency_ms_rank_2_p90") > 0)
    .withColumn("distinct_hosts_dense_rank", F.dense_rank().over(w5))
)

result = (
    summary
    .select(
        "k_count_latency_ms_tier",
        "host",
        "k",
        "host_max_latency_ms_rank_2_p90",
        "distinct_hosts",
        "distinct_hosts_dense_rank",
    )
    .orderBy(F.col("k_count_latency_ms_tier").desc_nulls_last())
)
