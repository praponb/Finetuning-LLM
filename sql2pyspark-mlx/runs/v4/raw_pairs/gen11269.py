from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())
w3 = Window.orderBy(F.col("val_dense_rank").desc_nulls_last())
w4 = Window.partitionBy("plan")
w5 = Window.partitionBy("plan").orderBy("val_dense_rank")
w6 = Window.partitionBy("plan").orderBy(F.col("val_dense_rank_dense_rank").desc_nulls_first())
w_recent = Window.partitionBy("plan").orderBy(F.col("val_filled").desc_nulls_last())
w8 = Window.orderBy(F.col("plan_stddev_samp_val_filled").desc())
w9 = Window.orderBy(F.col("val").asc_nulls_first())
w10 = Window.orderBy(F.col("val_dense_rank_dense_rank").desc_nulls_first())
w11 = Window.partitionBy("plan").orderBy(F.col("val_dense_rank").asc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("val"))
)

enriched = (
    flattened
    .select(
        "*",
        F.percent_rank().over(w).alias("val_percent_rank"),
        F.dense_rank().over(w2).alias("val_dense_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("currency") == "USD")
    .select(
        "id",
        "plan",
        "val",
        "val_dense_rank",
        (F.col("val") * F.col("val_dense_rank")).alias("val_times_val_dense_rank"),
        F.coalesce("val", F.lit(0)).alias("val_filled"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.rank().over(w3).alias("val_dense_rank_rank"),
        F.count("val").over(w4).alias("plan_count_val"),
        F.round((F.col("val_dense_rank") / F.sum("val_dense_rank").over(w4)), 4).alias("val_dense_rank_share"),
        F.dense_rank().over(w5).alias("val_dense_rank_dense_rank"),
    )
)

with_windows_price_tiers = (
    with_windows
    .select(
        "*",
        F.round((F.col("plan_count_val") / F.sum("plan_count_val").over(w4)), 4).alias("plan_count_val_share"),
        F.stddev_samp("val_filled").over(w4).alias("plan_stddev_samp_val_filled"),
        F.cume_dist().over(w6).alias("val_dense_rank_dense_rank_cume_dist"),
        F.cume_dist().over(w_recent).alias("val_filled_cume_dist"),
    )
)

with_windows_price_tiers_2 = (
    with_windows_price_tiers
    .withColumn("plan_max_plan_stddev_samp_val_filled", F.max("plan_stddev_samp_val_filled").over(w4))
    .withColumn("plan_sum_plan_stddev_samp_val_filled", F.sum("plan_stddev_samp_val_filled").over(w4))
)

scored = (
    with_windows_price_tiers_2
    .withColumn("plan_stddev_samp_val_filled_percent_rank", F.percent_rank().over(w8))
    .withColumn("val_dense_rank_share_2", F.round((F.col("val_dense_rank") / F.sum("val_dense_rank").over(w4)), 4))
    .withColumn("val_rank", F.rank().over(w9))
)

result = (
    scored
    .select(
        "*",
        F.rank().over(w10).alias("val_dense_rank_dense_rank_rank"),
        F.rank().over(w11).alias("val_dense_rank_rank_2"),
        F.sum("val_filled").over(w4).alias("plan_sum_val_filled"),
    )
)
