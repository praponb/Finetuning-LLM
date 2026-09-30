from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value_cume_dist").desc_nulls_last())
w_running = Window.partitionBy("currency")
w_grp = Window.orderBy(F.col("tier_value_cume_dist").desc_nulls_last())
w5 = Window.partitionBy("currency").orderBy(F.col("currency_sum_tier_value_cume_dist").desc_nulls_last())
w6 = Window.orderBy(F.col("tier_value_cume_dist_rank").asc_nulls_first())
w7 = Window.partitionBy("currency").orderBy(F.col("tier_value_cume_dist_dense_rank").desc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

filtered = (
    exploded
    .filter(F.col("effective_from") >= F.lit("2024-01-30").cast("date"))
    .withColumn("is_usd", F.when(F.col("currency") == "USD", 1).otherwise(0))
)

enriched = (
    filtered
    .filter(F.col("is_usd") > 43)
    .select(
        "id",
        "effective_from",
        "currency",
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
    )
)

metrics = (
    enriched
    .withColumn("tier_value_cume_dist_rank", F.rank().over(w2))
    .withColumn("currency_sum_tier_value_cume_dist", F.sum("tier_value_cume_dist").over(w_running))
    .withColumn("tier_value_cume_dist_dense_rank", F.dense_rank().over(w_grp))
)

scored = (
    metrics
    .select(
        "*",
        F.avg("tier_value_cume_dist_dense_rank").over(w_running).alias("currency_avg_tier_value_cume_dist_dense_rank"),
        F.percent_rank().over(w5).alias("currency_sum_tier_value_cume_dist_percent_rank"),
    )
)

base = (
    scored
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-03-07")))
    .select("*", F.substring("currency", 1, 2).alias("currency_prefix"))
)

metrics_price_tiers = (
    base
    .withColumn("tier_value_cume_dist_rank_percent_rank", F.percent_rank().over(w6))
    .withColumn("currency_avg_currency_avg_tier_value_cume_dist_dense_rank", F.avg("currency_avg_tier_value_cume_dist_dense_rank").over(w_running))
    .withColumn("tier_value_cume_dist_dense_rank_rank", F.rank().over(w7))
)

result = (
    metrics_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_value_cume_dist",
        "tier_value_cume_dist_rank",
        "tier_value_cume_dist_dense_rank",
        "currency_avg_tier_value_cume_dist_dense_rank",
        "currency_sum_tier_value_cume_dist_percent_rank",
        "currency_prefix",
    )
    .orderBy(F.col("tier_value_cume_dist").desc())
)
