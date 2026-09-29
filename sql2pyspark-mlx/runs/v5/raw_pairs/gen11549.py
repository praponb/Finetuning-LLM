from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.orderBy(F.col("tier_value").desc())
w_running = Window.partitionBy("currency")
w4 = Window.partitionBy("currency").orderBy("tier_value_percent_rank")
w5 = Window.partitionBy("currency").orderBy(F.col("currency_sum_currency_avg_tier_value").desc_nulls_last())
w6 = Window.orderBy(F.col("tier_value_cume_dist").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

with_windows = (
    exploded
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
    .withColumn("currency_avg_tier_value", F.avg("tier_value").over(w_running))
)

metrics = (
    with_windows
    .select(
        "*",
        F.sum("currency_avg_tier_value").over(w_running).alias("currency_sum_currency_avg_tier_value"),
        F.percent_rank().over(w).alias("tier_value_percent_rank_2"),
    )
)

metrics_price_tiers = (
    metrics
    .select(
        "effective_from",
        "currency",
        "currency_sum_currency_avg_tier_value",
        "tier_value_percent_rank_2",
        F.rank().over(w4).alias("tier_value_percent_rank_rank"),
        F.percent_rank().over(w5).alias("currency_sum_currency_avg_tier_value_percent_rank"),
        F.dense_rank().over(w6).alias("tier_value_cume_dist_dense_rank"),
    )
)

result = (
    metrics_price_tiers
    .select(
        "currency",
        "tier_value_percent_rank_rank",
        "tier_value_cume_dist_dense_rank",
    )
)
