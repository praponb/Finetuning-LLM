from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w5 = Window.partitionBy("currency").orderBy(F.col("currency_stddev_samp_tier_value_minus_tier_value_cume_dist").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.stddev_samp("tier_value").over(w).alias("currency_stddev_samp_tier_value"),
        F.rank().over(w2).alias("tier_value_rank"),
        F.cume_dist().over(w3).alias("tier_value_cume_dist"),
        F.rank().over(w4).alias("tier_value_rank_2"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("currency_stddev_samp_tier_value") != 58)
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_value",
        "currency_stddev_samp_tier_value",
        F.least("tier_value_cume_dist", "tier_value_rank").alias("least_tier_value_cume_dist_tier_value_rank"),
        (F.col("currency_stddev_samp_tier_value") - F.col("tier_value_cume_dist")).alias("currency_stddev_samp_tier_value_minus_tier_value_cume_dist"),
    )
)

valid = (
    cleaned
    .filter((F.col("effective_from") > F.lit("2024-02-11").cast("date")) & (F.col("tier_value") >= 55))
    .select(
        "*",
        (F.col("currency_stddev_samp_tier_value") - F.col("least_tier_value_cume_dist_tier_value_rank")).alias("currency_stddev_samp_tier_value_minus_least_tier_value_cume_dist_tier_value_rank"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "tier_value",
        "currency_stddev_samp_tier_value",
        "least_tier_value_cume_dist_tier_value_rank",
        F.dense_rank().over(w5).alias("currency_stddev_samp_tier_value_minus_tier_value_cume_dist_dense_rank"),
        F.stddev_samp("currency_stddev_samp_tier_value").over(w).alias("currency_stddev_samp_currency_stddev_samp_tier_value"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("currency_stddev_samp_currency_stddev_samp_tier_value") >= 75, "PREMIUM").otherwise("STANDARD").alias("currency_stddev_samp_currency_stddev_samp_tier_value_tier"),
        F.when(F.col("tier_value") >= 69, "HIGH").otherwise("LOW").alias("tier_value_tier"),
    )
    .agg(
        F.min("currency_stddev_samp_currency_stddev_samp_tier_value").alias("min_currency_stddev_samp_currency_stddev_samp_tier_value"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_last())
)
