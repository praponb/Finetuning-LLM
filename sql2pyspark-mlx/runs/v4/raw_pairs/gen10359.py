from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("tier_value").desc())
w_hist_2 = Window.orderBy(F.col("is_eur").desc_nulls_last())
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())
w_recent = Window.orderBy(F.col("tier_value").desc_nulls_last())
w5 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

filtered = (
    exploded
    .filter(F.col("tier_value") <= 76)
    .withColumn("is_eur", F.when(F.col("currency") == "EUR", 1).otherwise(0))
)

scored = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w_hist).alias("tier_value_dense_rank"),
        F.dense_rank().over(w_hist_2).alias("is_eur_dense_rank"),
        F.cume_dist().over(w3).alias("tier_value_cume_dist"),
        F.cume_dist().over(w_recent).alias("tier_value_cume_dist_2"),
        F.stddev_samp("tier_value").over(w5).alias("currency_stddev_samp_tier_value"),
    )
)

base = (
    scored
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-02-08")))
    .withColumn("adjusted_tier_value_dense_rank", F.round((F.col("tier_value_dense_rank") * 1.07), 2))
)

cleaned = (
    base
    .filter(F.col("effective_from") >= F.lit("2024-02-25").cast("date"))
    .withColumn("tier_value_dense_rank_times_tier_value", (F.col("tier_value_dense_rank") * F.col("tier_value")))
)

base_price_tiers = (
    cleaned
    .filter(F.col("tier_value_cume_dist_2") < 0.7)
    .withColumn("currency_title", F.initcap(F.trim("currency")))
)

result = (
    base_price_tiers
    .select(
        "effective_from",
        "tier_value",
        "is_eur",
        "tier_value_dense_rank",
        "is_eur_dense_rank",
        "tier_value_cume_dist",
        "tier_value_cume_dist_2",
        "adjusted_tier_value_dense_rank",
        "currency_title",
    )
)
