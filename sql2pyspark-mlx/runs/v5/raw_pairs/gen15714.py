from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_first())
w4 = Window.partitionBy("currency")
w5 = Window.orderBy(F.col("tier_value_dense_rank").desc_nulls_last())
w6 = Window.partitionBy("currency").orderBy(F.col("currency_avg_tier_value").desc())
w7 = Window.orderBy(F.col("tier_value_cume_dist").asc_nulls_last())
w_running_2 = Window.orderBy(F.col("tier_value_percent_rank").asc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_limits").alias("tier_value"))
)

scored = (
    flattened
    .select(
        "*",
        F.dense_rank().over(w).alias("tier_value_dense_rank"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
        F.cume_dist().over(w3).alias("tier_value_cume_dist"),
        F.avg("tier_value").over(w4).alias("currency_avg_tier_value"),
    )
)

filtered = (
    scored
    .filter(F.col("currency_avg_tier_value").between(32, 50))
    .select(
        "id",
        "currency",
        "tier_value_dense_rank",
        "tier_value_percent_rank",
        "tier_value_cume_dist",
        "currency_avg_tier_value",
        F.round("currency_avg_tier_value", 0).alias("currency_avg_tier_value_rounded"),
    )
)

scored_price_tiers = (
    filtered
    .filter(F.col("tier_value_percent_rank") >= 0.69)
    .withColumn("currency_count_tier_value_cume_dist", F.count("tier_value_cume_dist").over(w4))
    .withColumn("tier_value_dense_rank_rank", F.rank().over(w5))
)

scored_price_tiers_2 = (
    scored_price_tiers
    .withColumn("currency_avg_tier_value_rank", F.rank().over(w6))
    .withColumn("currency_avg_tier_value_percent_rank", F.avg("tier_value_percent_rank").over(w4))
    .withColumn("tier_value_cume_dist_dense_rank", F.dense_rank().over(w7))
    .withColumn("tier_value_percent_rank_cume_dist", F.cume_dist().over(w_running_2))
    .withColumn("currency_min_currency_avg_tier_value_rounded", F.min("currency_avg_tier_value_rounded").over(w4))
)

result = (
    scored_price_tiers_2
    .withColumn("currency_avg_tier_value_rank_tier", F.when(F.col("currency_avg_tier_value_rank") >= 7, "HIGH").otherwise("LOW"))
    .withColumn("tier_value_cume_dist_dense_rank_tier", F.when(F.col("tier_value_cume_dist_dense_rank") >= 9, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["currency_avg_tier_value_rank_tier", "tier_value_cume_dist_dense_rank_tier"], ["currency_avg_tier_value_rank_tier"], []],
        "currency_avg_tier_value_rank_tier",
        "tier_value_cume_dist_dense_rank_tier",
    )
    .agg(
        F.sum(F.when(F.col("currency") == "EUR", F.col("tier_value_cume_dist")).otherwise(0)).alias("eur_tier_value_cume_dist"),
        F.round(F.sum("currency_min_currency_avg_tier_value_rounded") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("currency_min_currency_avg_tier_value_rounded_per_thb"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
    )
    .orderBy(F.col("eur_tier_value_cume_dist").desc())
)
