from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w2 = Window.orderBy(F.col("tier_value").asc_nulls_first())
w3 = Window.partitionBy("currency")
w_hist = Window.orderBy("tier_value")
w5 = Window.orderBy("tier_value_cume_dist_2")
w6 = Window.orderBy("tier_value_rank")
w7 = Window.orderBy(F.col("tier_value_cume_dist_2").desc_nulls_last())
w_ordered = Window.orderBy(F.col("tier_value_cume_dist_2").asc_nulls_last())
w9 = Window.orderBy(F.col("tier_value_rank").desc_nulls_last())
w10 = Window.orderBy(F.col("tier_value_cume_dist_2_dense_rank").desc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

base = (
    flattened
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-02-19")))
    .withColumn("effective_from_label", F.date_format("effective_from", "yyyy-MM-dd"))
)

with_windows = (
    base
    .select(
        "id",
        "effective_from",
        F.rank().over(w_recent).alias("tier_value_rank"),
        F.cume_dist().over(w2).alias("tier_value_cume_dist"),
        F.min("tier_value").over(w3).alias("currency_min_tier_value"),
        F.cume_dist().over(w_hist).alias("tier_value_cume_dist_2"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w5).alias("tier_value_cume_dist_2_dense_rank"),
        F.rank().over(w6).alias("tier_value_rank_rank"),
        F.percent_rank().over(w7).alias("tier_value_cume_dist_2_percent_rank"),
    )
)

result = (
    metrics
    .filter(F.col("tier_value_rank_rank") > 4)
    .select(
        "id",
        "effective_from",
        "tier_value_rank",
        "currency_min_tier_value",
        "tier_value_rank_rank",
        "tier_value_cume_dist_2_percent_rank",
        F.rank().over(w_ordered).alias("tier_value_cume_dist_2_rank"),
        F.dense_rank().over(w9).alias("tier_value_rank_dense_rank"),
        F.percent_rank().over(w10).alias("tier_value_cume_dist_2_dense_rank_percent_rank"),
    )
)
