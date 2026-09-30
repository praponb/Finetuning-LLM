from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("k").orderBy(F.col("status_code").desc())
w3 = Window.orderBy(F.col("latency_ms").desc())
w4 = Window.orderBy("status_code")
w5 = Window.partitionBy("service")
w_running = Window.partitionBy("host").orderBy(F.col("status_code_cume_dist").asc_nulls_last())
w7 = Window.partitionBy("host")
w8 = Window.orderBy(F.col("status_code_cume_dist_dense_rank").desc())
w9 = Window.partitionBy("host").orderBy("status_code_cume_dist_dense_rank")
w10 = Window.partitionBy("host").orderBy(F.col("status_code_cume_dist_dense_rank").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

metrics = (
    flattened
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.dense_rank().over(w).alias("status_code_dense_rank"),
        F.rank().over(w2).alias("status_code_rank"),
        F.rank().over(w3).alias("latency_ms_rank"),
        F.cume_dist().over(w4).alias("status_code_cume_dist"),
        F.corr("latency_ms", "status_code").over(w5).alias("corr_latency_ms_status_code"),
    )
)

base = (
    metrics
    .filter(F.col("latency_ms_rank").between(3, 8))
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        "status_code_dense_rank",
        "latency_ms_rank",
        "status_code_cume_dist",
        F.coalesce("latency_ms_rank", F.lit(0)).alias("latency_ms_rank_filled"),
        (F.col("corr_latency_ms_status_code") + F.col("status_code_cume_dist")).alias("corr_latency_ms_status_code_plus_status_code_cume_dist"),
    )
)

enriched = (
    base
    .select(
        "id",
        "host",
        "status_code_cume_dist",
        F.dense_rank().over(w_running).alias("status_code_cume_dist_dense_rank"),
        F.stddev_samp("status_code_cume_dist").over(w7).alias("host_stddev_samp_status_code_cume_dist"),
    )
)

base_app_logs = (
    enriched
    .filter(F.col("status_code_cume_dist_dense_rank").between(6, 7))
    .select(
        "id",
        "host",
        "status_code_cume_dist_dense_rank",
        F.when(F.col("status_code_cume_dist_dense_rank") >= 8, "High").when(F.col("status_code_cume_dist_dense_rank") >= 4, "Medium").otherwise("Low").alias("status_code_cume_dist_dense_rank_level"),
    )
)

filtered = (
    base_app_logs
    .filter(F.col("status_code_cume_dist_dense_rank") > 7)
    .withColumn("host_title", F.initcap(F.trim("host")))
)

result = (
    filtered
    .select(
        "*",
        F.round((F.col("status_code_cume_dist_dense_rank") / F.sum("status_code_cume_dist_dense_rank").over(w7)), 4).alias("status_code_cume_dist_dense_rank_share"),
        F.dense_rank().over(w8).alias("status_code_cume_dist_dense_rank_dense_rank"),
        F.percent_rank().over(w9).alias("status_code_cume_dist_dense_rank_percent_rank"),
        F.cume_dist().over(w10).alias("status_code_cume_dist_dense_rank_cume_dist"),
    )
)
