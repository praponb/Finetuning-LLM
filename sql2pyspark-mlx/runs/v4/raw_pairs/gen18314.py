from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").asc_nulls_first())
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("host")
w4 = Window.partitionBy("service")
w5 = Window.partitionBy("host", "attr_key")
w_grp = Window.orderBy(F.col("latency_ms_cume_dist").desc_nulls_first())
w7 = Window.partitionBy("attr_key", "level")
w8 = Window.partitionBy("attr_key").orderBy(F.col("level_avg_latency_ms_num").asc_nulls_first())
w_ordered = Window.partitionBy("host").orderBy("status_code")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

metrics = (
    lines
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w))
    .withColumn("level_avg_latency_ms", F.avg("latency_ms").over(w2))
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w3))
    .withColumn("level_count_latency_ms", F.count("latency_ms").over(w2))
)

scored = (
    metrics
    .select(
        "*",
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4).alias("latency_ms_share"),
        F.round((F.col("level_avg_latency_ms") / F.sum("level_avg_latency_ms").over(w5)), 4).alias("level_avg_latency_ms_share"),
        F.dense_rank().over(w_grp).alias("latency_ms_cume_dist_dense_rank"),
        F.round((F.col("level_avg_latency_ms") / F.sum("level_avg_latency_ms").over(w7)), 4).alias("level_avg_latency_ms_share_2"),
        F.stddev_samp("corr_latency_ms_status_code").over(w3).alias("host_stddev_samp_corr_latency_ms_status_code"),
    )
)

cleaned = (
    scored
    .filter(F.col("latency_ms_share") > 0.32)
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        "attr_key",
        "v",
        "latency_ms_cume_dist",
        "level_avg_latency_ms",
        "corr_latency_ms_status_code",
        "level_count_latency_ms",
        "latency_ms_share",
        "level_avg_latency_ms_share",
        "latency_ms_cume_dist_dense_rank",
        "level_avg_latency_ms_share_2",
        F.col("level_avg_latency_ms").cast("double").alias("level_avg_latency_ms_num"),
    )
)

base = (
    cleaned
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        "attr_key",
        "latency_ms_cume_dist",
        "level_avg_latency_ms",
        "level_count_latency_ms",
        "latency_ms_share",
        "level_avg_latency_ms_share",
        "level_avg_latency_ms_share_2",
        "level_avg_latency_ms_num",
        (F.col("latency_ms_cume_dist_dense_rank") / F.nullif("level_avg_latency_ms_share", F.lit(0))).alias("latency_ms_cume_dist_dense_rank_per_level_avg_latency_ms_share"),
        F.col("level_count_latency_ms").cast("string").alias("level_count_latency_ms_num"),
    )
)

enriched = (
    base
    .withColumn("level_avg_latency_ms_num_dense_rank", F.dense_rank().over(w8))
    .withColumn("status_code_percent_rank", F.percent_rank().over(w_ordered))
)

result = (
    enriched
    .groupBy("host")
    .agg(
        F.count("*").alias("num_rows"),
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.count_if(F.col("host") == "h1").alias("h1_events"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
    )
    .filter(F.col("h1_events") >= 1)
    .orderBy("h1_events")
)
