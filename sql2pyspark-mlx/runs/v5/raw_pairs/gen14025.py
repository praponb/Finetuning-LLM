from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("latency_ms").desc_nulls_first())
w2 = Window.partitionBy("host", "service")
w3 = Window.partitionBy("service")
w4 = Window.orderBy("status_code_share_2")
w5 = Window.partitionBy("level")
w6 = Window.orderBy(F.col("corr_status_code_latency_ms").desc())
w7 = Window.partitionBy("level").orderBy(F.col("status_code_share").asc_nulls_first())
w8 = Window.orderBy(F.col("status_code_share_2").desc())
w9 = Window.partitionBy("host").orderBy(F.col("status_code_share").desc())
w10 = Window.partitionBy("k").orderBy(F.col("status_code_share_2_dense_rank").asc_nulls_first())
w11 = Window.partitionBy("k", "service")
w12 = Window.partitionBy("host").orderBy(F.col("status_code_share").asc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

valid = (
    lines
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.substring("host", 1, 4).alias("host_prefix"),
        F.upper("host").alias("host_upper"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.dense_rank().over(w).alias("latency_ms_dense_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.sum("latency_ms").over(w3).alias("service_sum_latency_ms"),
        F.corr("status_code", "latency_ms").over(w3).alias("corr_status_code_latency_ms"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share_2"),
    )
)

enriched = (
    metrics
    .select(
        "service",
        "host",
        "status_code",
        "k",
        "host_prefix",
        "host_upper",
        "status_code_share",
        "corr_status_code_latency_ms",
        "status_code_share_2",
        F.dense_rank().over(w4).alias("status_code_share_2_dense_rank"),
        F.round((F.col("latency_ms_dense_rank") / F.sum("latency_ms_dense_rank").over(w5)), 4).alias("latency_ms_dense_rank_share"),
        F.rank().over(w6).alias("corr_status_code_latency_ms_rank"),
        F.dense_rank().over(w7).alias("status_code_share_dense_rank"),
        F.cume_dist().over(w8).alias("status_code_share_2_cume_dist"),
    )
)

enriched_app_logs = (
    enriched
    .filter(F.col("service") == "search")
    .select(
        "service",
        "k",
        "host_prefix",
        "status_code_share",
        "corr_status_code_latency_ms",
        "status_code_share_2",
        "status_code_share_2_dense_rank",
        "corr_status_code_latency_ms_rank",
        "status_code_share_dense_rank",
        F.cume_dist().over(w9).alias("status_code_share_cume_dist"),
        F.rank().over(w10).alias("status_code_share_2_dense_rank_rank"),
        F.stddev_samp("latency_ms_dense_rank_share").over(w11).alias("k_stddev_samp_latency_ms_dense_rank_share"),
        F.rank().over(w12).alias("status_code_share_rank"),
    )
)

base = (
    enriched_app_logs
    .filter((~F.col("k").eqNullSafe(F.lit("region"))) & (F.col("k_stddev_samp_latency_ms_dense_rank_share") < 0))
    .withColumn("corr_status_code_latency_ms_plus_status_code_share_2_dense_rank_rank", (F.col("corr_status_code_latency_ms") + F.col("status_code_share_2_dense_rank_rank")))
    .withColumn("least_corr_status_code_latency_ms_status_code_share_cume_dist", F.least("corr_status_code_latency_ms", "status_code_share_cume_dist"))
    .withColumn("k_clean", F.coalesce("k", F.lit("UNKNOWN")))
)

valid_app_logs = (
    base
    .filter(F.col("k").isNotNull() & (F.col("status_code_share_dense_rank") != 3))
    .select(
        "service",
        "host_prefix",
        "status_code_share",
        "corr_status_code_latency_ms",
        "status_code_share_2",
        "status_code_share_cume_dist",
        "status_code_share_2_dense_rank_rank",
        "k_stddev_samp_latency_ms_dense_rank_share",
        "corr_status_code_latency_ms_plus_status_code_share_2_dense_rank_rank",
        "least_corr_status_code_latency_ms_status_code_share_cume_dist",
        "k_clean",
        F.substring("service", 1, 4).alias("service_prefix"),
    )
)

result = (
    valid_app_logs
    .groupBy("service")
    .agg(
        F.round(F.sum("least_corr_status_code_latency_ms_status_code_share_cume_dist") / F.nullif(F.count(F.when(F.col("service") == "checkout", 1)), F.lit(0)), 2).alias("least_corr_status_code_latency_ms_status_code_share_cume_dist_per_checkout"),
        F.count(F.when(F.col("service") == "checkout", 1)).alias("checkout_count"),
        F.avg(F.when(F.col("service") == "billing", F.col("status_code_share"))).alias("billing_avg_status_code_share"),
        F.percentile_approx("status_code_share", 0.95).alias("status_code_share_p95"),
        F.round(F.sum("status_code_share") / F.nullif(F.count(F.when(F.col("service") == "search", 1)), F.lit(0)), 2).alias("status_code_share_per_search"),
    )
    .filter(F.col("billing_avg_status_code_share") >= 0)
    .orderBy(
        F.col("least_corr_status_code_latency_ms_status_code_share_cume_dist_per_checkout").desc(),
    )
)
