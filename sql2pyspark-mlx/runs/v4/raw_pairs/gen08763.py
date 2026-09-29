from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("latency_ms").desc_nulls_last())
w2 = Window.partitionBy("k", "service")
w3 = Window.partitionBy("level").orderBy(F.col("k_max_latency_ms").desc_nulls_last())
w_part = Window.orderBy(F.col("k_max_latency_ms").desc_nulls_last())
w5 = Window.partitionBy("service").orderBy(F.col("latency_ms_rank").desc_nulls_first())
w6 = Window.partitionBy("service")
w7 = Window.partitionBy("level")
w_recent = Window.orderBy(F.col("latency_ms_share").asc_nulls_last())
w9 = Window.partitionBy("level", "k")
w10 = Window.orderBy(F.col("latency_ms_rank").desc_nulls_first())
w11 = Window.partitionBy("level").orderBy(F.col("latency_ms_share").asc_nulls_last())
w12 = Window.orderBy(F.col("latency_ms_rank_cume_dist").desc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

enriched = (
    lines
    .select(
        "*",
        F.rank().over(w).alias("latency_ms_rank"),
        F.max("latency_ms").over(w2).alias("k_max_latency_ms"),
    )
)

cleaned = (
    enriched
    .filter(F.col("level") != "ERROR")
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "k",
        "latency_ms_rank",
        "k_max_latency_ms",
        F.when(F.col("latency_ms") >= 2667, "High").when(F.col("latency_ms") >= 864, "Medium").otherwise("Low").alias("latency_ms_level"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled"),
    )
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.trim("service").alias("clean_service"),
        F.upper("k").alias("k_upper"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled_2"),
    )
)

with_windows = (
    cleaned_app_logs
    .select(
        "*",
        F.rank().over(w3).alias("k_max_latency_ms_rank"),
        F.rank().over(w_part).alias("k_max_latency_ms_rank_2"),
        F.cume_dist().over(w5).alias("latency_ms_rank_cume_dist"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w6)), 4).alias("latency_ms_share"),
        F.round((F.col("k_max_latency_ms") / F.sum("k_max_latency_ms").over(w7)), 4).alias("k_max_latency_ms_share"),
    )
)

matched_keys = (
    with_windows
    .select("service")
    .exceptAll(
        df_app_logs
        .filter(F.col("status_code") >= 327)
        .select("service")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("latency_ms_share_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("level_stddev_samp_latency_ms_share", F.stddev_samp("latency_ms_share").over(w9))
    .withColumn("latency_ms_rank_percent_rank", F.percent_rank().over(w10))
    .withColumn("latency_ms_share_dense_rank", F.dense_rank().over(w11))
    .withColumn("latency_ms_rank_cume_dist_cume_dist", F.cume_dist().over(w12))
)
