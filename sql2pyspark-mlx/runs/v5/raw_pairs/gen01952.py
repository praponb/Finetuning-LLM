from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_first())
w_part = Window.orderBy(F.col("val").desc_nulls_last())
w3 = Window.orderBy(F.col("val").asc_nulls_first())
w4 = Window.partitionBy("currency")
w5 = Window.orderBy(F.col("val_dense_rank").desc_nulls_last())
w6 = Window.partitionBy("currency").orderBy(F.col("val_cume_dist").desc_nulls_first())
w_recent = Window.partitionBy("currency").orderBy(F.col("val_rank").desc_nulls_last())
w8 = Window.partitionBy("currency").orderBy(F.col("currency_max_currency_avg_val_cume_dist").asc_nulls_last())
w9 = Window.partitionBy("currency").orderBy(F.col("val_dense_rank_share").desc_nulls_last())
w10 = Window.orderBy(F.col("currency_max_currency_avg_val_cume_dist").desc())
w11 = Window.orderBy("val_cume_dist")

lines = df_price_tiers.select("id", "currency", F.explode_outer("tier_limits").alias("val"))

scored = (
    lines
    .filter(F.col("currency") == "USD")
    .select(
        "id",
        "currency",
        F.rank().over(w_hist).alias("val_rank"),
        F.dense_rank().over(w_part).alias("val_dense_rank"),
        F.cume_dist().over(w3).alias("val_cume_dist"),
        F.rank().over(w_hist).alias("val_rank_2"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.avg("val_cume_dist").over(w4).alias("currency_avg_val_cume_dist"),
        F.round((F.col("val_dense_rank") / F.sum("val_dense_rank").over(w4)), 4).alias("val_dense_rank_share"),
        F.round((F.col("val_rank") / F.sum("val_rank").over(w4)), 4).alias("val_rank_share"),
        F.dense_rank().over(w5).alias("val_dense_rank_dense_rank"),
        F.round((F.col("val_rank_2") / F.sum("val_rank_2").over(w4)), 4).alias("val_rank_2_share"),
    )
)

valid = (
    metrics
    .filter(F.col("currency") != "THB")
    .select(
        "currency",
        "val_rank",
        "val_dense_rank",
        "val_cume_dist",
        "currency_avg_val_cume_dist",
        "val_dense_rank_share",
        "val_rank_2_share",
        F.when(F.col("currency") == "USD", 1).otherwise(0).alias("is_usd"),
    )
)

metrics_price_tiers = (
    valid
    .select(
        "*",
        F.cume_dist().over(w6).alias("val_cume_dist_cume_dist"),
        F.cume_dist().over(w_recent).alias("val_rank_cume_dist"),
        F.max("currency_avg_val_cume_dist").over(w4).alias("currency_max_currency_avg_val_cume_dist"),
    )
)

with_windows = (
    metrics_price_tiers
    .select(
        "currency",
        "val_dense_rank",
        "val_cume_dist",
        "currency_avg_val_cume_dist",
        "is_usd",
        "val_cume_dist_cume_dist",
        F.rank().over(w8).alias("currency_max_currency_avg_val_cume_dist_rank"),
        F.dense_rank().over(w9).alias("val_dense_rank_share_dense_rank"),
        F.dense_rank().over(w10).alias("currency_max_currency_avg_val_cume_dist_dense_rank"),
        F.rank().over(w11).alias("val_cume_dist_rank"),
        F.sum("currency_avg_val_cume_dist").over(w4).alias("currency_sum_currency_avg_val_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "currency",
        "val_dense_rank",
        "currency_avg_val_cume_dist",
        "is_usd",
        "val_cume_dist_cume_dist",
        "val_dense_rank_share_dense_rank",
        "val_cume_dist_rank",
    )
    .orderBy(F.col("currency").desc_nulls_first())
)
