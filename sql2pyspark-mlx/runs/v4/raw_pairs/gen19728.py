from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w2 = Window.partitionBy("currency")
w3 = Window.orderBy(F.col("tier_value").desc())
w4 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w5 = Window.orderBy(F.col("tier_value").asc_nulls_first())
w6 = Window.partitionBy("currency").orderBy(F.col("currency_max_tier_value").desc_nulls_last())
w7 = Window.partitionBy("currency").orderBy("tier_value_rank_share")
w_grp = Window.orderBy(F.col("tier_value_percent_rank_2").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("discounts").alias("tier_value"))
)

enriched = (
    lines
    .select(
        "*",
        F.rank().over(w).alias("tier_value_rank"),
        F.max("tier_value").over(w2).alias("currency_max_tier_value"),
        F.percent_rank().over(w3).alias("tier_value_percent_rank"),
        F.dense_rank().over(w4).alias("tier_value_dense_rank"),
    )
)

metrics = (
    enriched
    .withColumn("tier_value_dense_rank_share", F.round((F.col("tier_value_dense_rank") / F.sum("tier_value_dense_rank").over(w2)), 4))
    .withColumn("tier_value_percent_rank_2", F.percent_rank().over(w5))
    .withColumn("tier_value_rank_share", F.round((F.col("tier_value_rank") / F.sum("tier_value_rank").over(w2)), 4))
    .withColumn("currency_max_tier_value_cume_dist", F.cume_dist().over(w6))
    .withColumn("currency_stddev_samp_tier_value_dense_rank", F.stddev_samp("tier_value_dense_rank").over(w2))
)

matched_keys = (
    metrics
    .select("currency")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from") >= "2023-03-01")
        .select("currency")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("currency_min_tier_value_rank_share", F.min("tier_value_rank_share").over(w2))
    .withColumn("tier_value_rank_share_dense_rank", F.dense_rank().over(w7))
)

filtered = (
    scored
    .filter(F.col("currency_max_tier_value") <= 69)
    .withColumn("currency_prefix", F.substring("currency", 1, 2))
)

enriched_price_tiers = (
    filtered
    .select(
        "*",
        F.cume_dist().over(w_grp).alias("tier_value_percent_rank_2_cume_dist"),
        F.round((F.col("currency_stddev_samp_tier_value_dense_rank") / F.sum("currency_stddev_samp_tier_value_dense_rank").over(w2)), 4).alias("currency_stddev_samp_tier_value_dense_rank_share"),
    )
)

result = (
    enriched_price_tiers
    .withColumn("currency_max_tier_value_cume_dist_tier", F.when(F.col("currency_max_tier_value_cume_dist") >= 0.74, "PREMIUM").otherwise("STANDARD"))
    .withColumn("tier_value_dense_rank_share_tier", F.when(F.col("tier_value_dense_rank_share") >= 0.69, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["currency_max_tier_value_cume_dist_tier", "tier_value_dense_rank_share_tier", "currency"], ["currency_max_tier_value_cume_dist_tier", "tier_value_dense_rank_share_tier"], ["currency_max_tier_value_cume_dist_tier"]],
        "currency_max_tier_value_cume_dist_tier",
        "tier_value_dense_rank_share_tier",
        "currency",
    )
    .agg(
        F.sum(F.when(F.col("currency") == "EUR", F.col("tier_value_dense_rank")).otherwise(0)).alias("eur_tier_value_dense_rank"),
        F.sum(F.when(F.col("currency") == "USD", F.col("tier_value_rank_share_dense_rank"))).alias("usd_total_tier_value_rank_share_dense_rank"),
        F.grouping("currency_max_tier_value_cume_dist_tier").alias("gid"),
    )
)
