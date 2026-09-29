from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.orderBy(F.col("tier_value").desc())
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("currency").orderBy(F.col("tier_value_rank").asc_nulls_last())
w5 = Window.partitionBy("currency").orderBy("tier_value")
w6 = Window.partitionBy("currency").orderBy("tier_value_rank")
w7 = Window.partitionBy("currency").orderBy(F.col("tier_value_percent_rank").asc_nulls_last())
w8 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_first())
w9 = Window.partitionBy("currency").orderBy(F.col("tier_value_rank").desc())
w10 = Window.partitionBy("currency").orderBy(F.col("tier_value_cume_dist").desc())
w_hist = Window.orderBy(F.col("tier_value_rank_dense_rank").desc())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

scored = (
    exploded
    .select(
        "*",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.rank().over(w2).alias("tier_value_rank"),
        F.cume_dist().over(w).alias("tier_value_cume_dist"),
        F.min("tier_value").over(w3).alias("currency_min_tier_value"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.dense_rank().over(w4).alias("tier_value_rank_dense_rank"),
        F.dense_rank().over(w5).alias("tier_value_dense_rank"),
    )
)

scored_price_tiers = (
    metrics
    .select(
        "*",
        F.round((F.col("tier_value_dense_rank") / F.sum("tier_value_dense_rank").over(w3)), 4).alias("tier_value_dense_rank_share"),
        F.cume_dist().over(w6).alias("tier_value_rank_cume_dist"),
        F.dense_rank().over(w7).alias("tier_value_percent_rank_dense_rank"),
        F.cume_dist().over(w8).alias("tier_value_cume_dist_2"),
        F.rank().over(w9).alias("tier_value_rank_rank"),
    )
)

scored_price_tiers_2 = (
    scored_price_tiers
    .withColumn("currency_sum_tier_value_percent_rank", F.sum("tier_value_percent_rank").over(w3))
    .withColumn("tier_value_cume_dist_dense_rank", F.dense_rank().over(w10))
    .withColumn("tier_value_rank_dense_rank_percent_rank", F.percent_rank().over(w_hist))
)

result = (
    scored_price_tiers_2
    .cube(
        F.when(F.col("tier_value_rank_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("tier_value_rank_dense_rank_tier"),
        F.when(F.col("tier_value_cume_dist_dense_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("tier_value_cume_dist_dense_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.max(F.when(F.col("currency") == "USD", F.col("tier_value_percent_rank_dense_rank"))).alias("usd_max_tier_value_percent_rank_dense_rank"),
        F.max(F.when(F.col("currency") == "EUR", F.col("tier_value_rank_dense_rank_percent_rank"))).alias("eur_max_tier_value_rank_dense_rank_percent_rank"),
        F.round(F.avg("tier_value_dense_rank_share"), 2).alias("avg_tier_value_dense_rank_share"),
        F.percentile_approx("currency_min_tier_value", 0.95).alias("currency_min_tier_value_p95"),
    )
)
