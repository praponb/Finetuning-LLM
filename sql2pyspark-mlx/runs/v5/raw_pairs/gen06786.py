from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan")
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("val").asc_nulls_last())
w3 = Window.orderBy(F.col("val").asc_nulls_first())
w4 = Window.orderBy(F.col("val").desc_nulls_first())
w_recent = Window.partitionBy("currency")
w6 = Window.partitionBy("plan", "currency").orderBy(F.col("val_rank").desc_nulls_last())
w7 = Window.partitionBy("currency", "plan")
w8 = Window.partitionBy("plan").orderBy(F.col("val_dense_rank_share_3").desc())
w9 = Window.partitionBy("plan").orderBy(F.col("plan_min_val").desc())
w10 = Window.orderBy(F.col("val_dense_rank_share_3").desc())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("val"))
)

with_windows = (
    flattened
    .withColumn("plan_min_val", F.min("val").over(w_ordered))
    .withColumn("val_rank", F.rank().over(w2))
)

enriched = with_windows.select("*", F.percent_rank().over(w3).alias("val_percent_rank"))

with_windows_price_tiers = (
    enriched
    .select(
        "*",
        F.dense_rank().over(w4).alias("val_dense_rank"),
        F.sum("val").over(w_recent).alias("currency_sum_val"),
    )
)

metrics = (
    with_windows_price_tiers
    .select(
        "*",
        F.round((F.col("val_dense_rank") / F.sum("val_dense_rank").over(w_recent)), 4).alias("val_dense_rank_share"),
    )
)

with_windows_price_tiers_2 = (
    metrics
    .select(
        "plan",
        "plan_min_val",
        "val_dense_rank",
        "currency_sum_val",
        F.round((F.col("val_dense_rank") / F.sum("val_dense_rank").over(w_recent)), 4).alias("val_dense_rank_share_2"),
        F.round((F.col("val_dense_rank") / F.sum("val_dense_rank").over(w_recent)), 4).alias("val_dense_rank_share_3"),
        F.percent_rank().over(w6).alias("val_rank_percent_rank"),
        F.count("val_rank").over(w7).alias("currency_count_val_rank"),
    )
)

scored = (
    with_windows_price_tiers_2
    .select(
        "plan_min_val",
        "currency_sum_val",
        "val_dense_rank_share_2",
        "val_dense_rank_share_3",
        "currency_count_val_rank",
        F.cume_dist().over(w8).alias("val_dense_rank_share_3_cume_dist"),
        F.sum("val_rank_percent_rank").over(w_ordered).alias("plan_sum_val_rank_percent_rank"),
        F.percent_rank().over(w9).alias("plan_min_val_percent_rank"),
        F.dense_rank().over(w10).alias("val_dense_rank_share_3_dense_rank"),
    )
)

result = (
    scored
    .select(
        "currency_sum_val",
        "val_dense_rank_share_2",
        "val_dense_rank_share_3",
        "currency_count_val_rank",
        "plan_sum_val_rank_percent_rank",
        "plan_min_val_percent_rank",
        "val_dense_rank_share_3_dense_rank",
    )
    .orderBy(
        F.col("plan_min_val_percent_rank").desc(),
        "plan_sum_val_rank_percent_rank",
    )
)
