from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w3 = Window.partitionBy("currency")
w4 = Window.orderBy(F.col("currency_stddev_samp_tier_value").desc_nulls_last())
w5 = Window.orderBy(F.col("currency_stddev_samp_tier_value").desc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

with_windows = (
    lines
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w))
    .withColumn("tier_value_rank", F.rank().over(w2))
    .withColumn("currency_stddev_samp_tier_value", F.stddev_samp("tier_value").over(w3))
)

filtered = (
    with_windows
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-10-19")))
    .select(
        "*",
        F.round((F.col("tier_value_rank") * 0.85), 2).alias("tier_value_rank_adjusted"),
        (F.col("tier_value_rank") / F.nullif("currency_stddev_samp_tier_value", F.lit(0))).alias("tier_value_rank_per_currency_stddev_samp_tier_value"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.percent_rank().over(w4).alias("currency_stddev_samp_tier_value_percent_rank"),
        F.cume_dist().over(w5).alias("currency_stddev_samp_tier_value_cume_dist"),
    )
)

result = (
    scored
    .select(
        "id",
        "currency",
        "tier_value_dense_rank",
        "tier_value_rank",
        "currency_stddev_samp_tier_value",
        "tier_value_rank_adjusted",
        "tier_value_rank_per_currency_stddev_samp_tier_value",
        "currency_stddev_samp_tier_value_percent_rank",
        "currency_stddev_samp_tier_value_cume_dist",
        F.when(F.col("currency").rlike("^(USD|THB)$"), "CORE").when(F.col("currency").rlike("^(EUR)$"), "GROUP_A").otherwise("PRIORITY").alias("currency_zone"),
    )
    .orderBy(F.col("tier_value_rank_adjusted").desc_nulls_first(), "currency")
)
