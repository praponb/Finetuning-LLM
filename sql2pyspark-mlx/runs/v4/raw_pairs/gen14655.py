from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.orderBy(F.col("adjusted_tier_value").asc_nulls_first())
w3 = Window.orderBy(F.col("tier_value").asc_nulls_last())
w4 = Window.orderBy(F.col("adjusted_tier_value").desc_nulls_last())
w5 = Window.orderBy(F.col("adjusted_tier_value").desc_nulls_first())
w6 = Window.orderBy(F.col("tier_value_dense_rank").desc_nulls_first())
w7 = Window.orderBy(F.col("adjusted_tier_value_cume_dist_2").asc_nulls_last())
w8 = Window.orderBy(F.col("tier_value").asc_nulls_first())
w_ordered = Window.orderBy(F.col("tier_value_dense_rank_rank").asc_nulls_first())
w10 = Window.orderBy(F.col("adjusted_tier_value_cume_dist_2").desc_nulls_last())
w11 = Window.orderBy(F.col("adjusted_tier_value_percent_rank").desc())
w12 = Window.orderBy(F.col("adjusted_tier_value").desc())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("tier_value"))
)

filtered = (
    lines
    .filter(F.col("effective_from") > F.lit("2024-04-21").cast("date"))
    .withColumn("adjusted_tier_value", F.round((F.col("tier_value") * 1.15), 2))
)

metrics = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.cume_dist().over(w2).alias("adjusted_tier_value_cume_dist"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank_2"),
        F.percent_rank().over(w4).alias("adjusted_tier_value_percent_rank"),
        F.cume_dist().over(w5).alias("adjusted_tier_value_cume_dist_2"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.rank().over(w6).alias("tier_value_dense_rank_rank"),
        F.percent_rank().over(w7).alias("adjusted_tier_value_cume_dist_2_percent_rank"),
        F.rank().over(w8).alias("tier_value_rank"),
    )
)

metrics_price_tiers = (
    with_windows
    .withColumn("tier_value_dense_rank_rank_rank", F.rank().over(w_ordered))
    .withColumn("adjusted_tier_value_cume_dist_2_dense_rank", F.dense_rank().over(w10))
)

with_windows_price_tiers = (
    metrics_price_tiers
    .withColumn("adjusted_tier_value_percent_rank_dense_rank", F.dense_rank().over(w11))
    .withColumn("adjusted_tier_value_percent_rank_2", F.percent_rank().over(w12))
    .withColumn("adjusted_tier_value_cume_dist_3", F.cume_dist().over(w5))
)

result = (
    with_windows_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_value",
        "adjusted_tier_value",
        "tier_value_dense_rank",
        "adjusted_tier_value_cume_dist",
        "adjusted_tier_value_percent_rank",
        "adjusted_tier_value_cume_dist_2_percent_rank",
        "tier_value_dense_rank_rank_rank",
    )
    .orderBy("adjusted_tier_value_cume_dist_2_percent_rank")
)
