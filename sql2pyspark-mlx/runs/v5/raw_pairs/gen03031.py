from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value_filled").desc_nulls_first())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value_filled").desc_nulls_last())
w4 = Window.orderBy(F.col("tier_value_filled_dense_rank").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

filtered = (
    lines
    .filter(F.col("effective_from") >= F.lit("2024-03-23").cast("date"))
    .select(
        "*",
        F.coalesce("tier_value", F.lit(0)).alias("tier_value_filled"),
        F.lower("currency").alias("currency_lower"),
    )
)

remaining_keys = (
    filtered
    .select("currency")
    .intersectAll(
        df_price_tiers
        .filter(F.col("plan").isin(["pro", "basic"]))
        .select("currency")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .filter(F.month("effective_from").isin([4, 5, 12]))
    .select(
        "*",
        F.dense_rank().over(w).alias("tier_value_filled_dense_rank"),
        F.stddev_samp("tier_value").over(w2).alias("currency_stddev_samp_tier_value"),
    )
)

filtered_price_tiers = (
    metrics
    .filter(F.col("currency_stddev_samp_tier_value").between(44, 53))
    .select("*", F.when(F.col("currency") == "THB", 1).otherwise(0).alias("is_thb"))
)

base = (
    filtered_price_tiers
    .filter(F.col("effective_from") > F.lit("2024-03-01").cast("date"))
    .select(
        "effective_from",
        "currency",
        "tier_value_filled",
        "currency_lower",
        "tier_value_filled_dense_rank",
        "currency_stddev_samp_tier_value",
        "is_thb",
        F.date_trunc("year", "effective_from").alias("effective_from_period"),
        F.col("currency_stddev_samp_tier_value").cast("double").alias("currency_stddev_samp_tier_value_num"),
    )
)

result = (
    base
    .select(
        "currency_lower",
        "currency_stddev_samp_tier_value",
        "effective_from_period",
        "currency_stddev_samp_tier_value_num",
        F.percent_rank().over(w3).alias("tier_value_filled_percent_rank"),
        F.dense_rank().over(w4).alias("tier_value_filled_dense_rank_dense_rank"),
        F.avg("tier_value_filled").over(w2).alias("currency_avg_tier_value_filled"),
    )
)
