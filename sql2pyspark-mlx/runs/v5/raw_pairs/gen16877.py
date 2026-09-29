from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("currency")
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").desc_nulls_first())
w_hist = Window.orderBy(F.col("tier_value").desc())
w5 = Window.partitionBy("plan", "currency").orderBy(F.col("tier_value").desc_nulls_last())
w6 = Window.partitionBy("currency").orderBy(F.col("currency_sum_tier_value_dense_rank_per_tier_value_dense_rank_3_filled").desc_nulls_first())
w7 = Window.orderBy(F.col("currency_sum_tier_value_dense_rank_per_tier_value_dense_rank_3_filled").desc_nulls_first())
w8 = Window.orderBy(F.col("tier_value_dense_rank_3").desc_nulls_first())
w9 = Window.orderBy(F.col("tier_value_dense_rank_per_tier_value_dense_rank_3_filled").asc_nulls_first())
w10 = Window.orderBy(F.col("tier_value_dense_rank_per_tier_value_dense_rank_3_filled_rank").asc_nulls_last())
w11 = Window.orderBy(F.col("tier_value_dense_rank_3_percent_rank").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("tier_value"))
)

enriched = (
    lines
    .select(
        "id",
        "currency",
        "tier_value",
        F.stddev_samp("tier_value").over(w_recent).alias("currency_stddev_samp_tier_value"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank_2"),
        F.dense_rank().over(w_hist).alias("tier_value_dense_rank_3"),
        F.cume_dist().over(w5).alias("tier_value_cume_dist"),
    )
)

cleaned = (
    enriched
    .filter(F.col("tier_value_dense_rank_3") < 4)
    .withColumn("tier_value_dense_rank_3_filled", F.coalesce("tier_value_dense_rank_3", F.lit(0)))
)

filtered = (
    cleaned
    .filter(F.col("tier_value_dense_rank_3") > 5)
    .select(
        "id",
        "currency",
        "tier_value",
        "currency_stddev_samp_tier_value",
        "tier_value_dense_rank",
        "tier_value_dense_rank_3",
        (F.col("tier_value_dense_rank") / F.nullif("tier_value_dense_rank_3_filled", F.lit(0))).alias("tier_value_dense_rank_per_tier_value_dense_rank_3_filled"),
        (F.col("tier_value") - F.col("currency_stddev_samp_tier_value")).alias("tier_value_minus_currency_stddev_samp_tier_value"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.sum("tier_value_dense_rank_per_tier_value_dense_rank_3_filled").over(w_recent).alias("currency_sum_tier_value_dense_rank_per_tier_value_dense_rank_3_filled"),
        F.stddev_samp("currency_stddev_samp_tier_value").over(w_recent).alias("currency_stddev_samp_currency_stddev_samp_tier_value"),
    )
)

metrics = (
    scored
    .select(
        "tier_value",
        "currency_stddev_samp_tier_value",
        "currency_stddev_samp_currency_stddev_samp_tier_value",
        F.dense_rank().over(w6).alias("currency_sum_tier_value_dense_rank_per_tier_value_dense_rank_3_filled_dense_rank"),
        F.dense_rank().over(w7).alias("currency_sum_tier_value_dense_rank_per_tier_value_dense_rank_3_filled_dense_rank_2"),
        F.percent_rank().over(w8).alias("tier_value_dense_rank_3_percent_rank"),
        F.min("tier_value_minus_currency_stddev_samp_tier_value").over(w_recent).alias("currency_min_tier_value_minus_currency_stddev_samp_tier_value"),
        F.rank().over(w9).alias("tier_value_dense_rank_per_tier_value_dense_rank_3_filled_rank"),
    )
)

scored_price_tiers = (
    metrics
    .filter(F.col("currency_stddev_samp_currency_stddev_samp_tier_value") >= 77)
    .select(
        "currency_stddev_samp_tier_value",
        "currency_stddev_samp_currency_stddev_samp_tier_value",
        "currency_sum_tier_value_dense_rank_per_tier_value_dense_rank_3_filled_dense_rank",
        "currency_min_tier_value_minus_currency_stddev_samp_tier_value",
        F.cume_dist().over(w10).alias("tier_value_dense_rank_per_tier_value_dense_rank_3_filled_rank_cume_dist"),
        F.rank().over(w11).alias("tier_value_dense_rank_3_percent_rank_rank"),
    )
)

result = (
    scored_price_tiers
    .select(
        "currency_stddev_samp_tier_value",
        "currency_min_tier_value_minus_currency_stddev_samp_tier_value",
        "tier_value_dense_rank_3_percent_rank_rank",
    )
)
