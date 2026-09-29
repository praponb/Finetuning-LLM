from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w4 = Window.orderBy(F.col("currency_stddev_samp_tier_value").desc())
w5 = Window.orderBy(F.col("currency_stddev_samp_tier_value").asc_nulls_first())
w6 = Window.orderBy(F.col("tier_value").desc())

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_limits").alias("tier_value"))
)

matched_keys = (
    exploded
    .select("id", "currency")
    .intersect(
        df_price_tiers
        .filter(F.year("effective_from") >= 2025)
        .select("id", "currency")
    )
)

reconciled = (
    exploded
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.stddev_samp("tier_value").over(w_ordered).alias("currency_stddev_samp_tier_value"),
        F.min("tier_value").over(w_ordered).alias("currency_min_tier_value"),
    )
)

enriched_price_tiers = (
    enriched
    .select(
        "*",
        F.rank().over(w2).alias("tier_value_rank"),
        F.cume_dist().over(w3).alias("tier_value_cume_dist"),
    )
)

metrics = (
    enriched_price_tiers
    .select(
        "*",
        F.dense_rank().over(w4).alias("currency_stddev_samp_tier_value_dense_rank"),
        F.cume_dist().over(w5).alias("currency_stddev_samp_tier_value_cume_dist"),
        F.dense_rank().over(w6).alias("tier_value_dense_rank"),
        F.round((F.col("tier_value_rank") / F.sum("tier_value_rank").over(w_ordered)), 4).alias("tier_value_rank_share"),
    )
)

enriched_price_tiers_2 = (
    metrics
    .withColumn("currency_avg_currency_min_tier_value", F.avg("currency_min_tier_value").over(w_ordered))
    .withColumn("currency_count_currency_min_tier_value", F.count("currency_min_tier_value").over(w_ordered))
)

result = (
    enriched_price_tiers_2
    .select(
        "id",
        "currency",
        "currency_stddev_samp_tier_value",
        "currency_min_tier_value",
        "tier_value_rank",
        "currency_stddev_samp_tier_value_dense_rank",
        "tier_value_rank_share",
        "currency_avg_currency_min_tier_value",
        "currency_count_currency_min_tier_value",
        F.when(F.col("currency").rlike("^(THB|USD)$"), "PRIORITY").otherwise("EXTENDED").alias("currency_zone"),
    )
)
