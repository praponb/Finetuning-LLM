from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())
w4 = Window.partitionBy("plan")
w5 = Window.partitionBy("plan").orderBy(F.col("tier_value_dense_rank_per_currency_avg_tier_value").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

scored = (
    lines
    .filter(~F.col("currency").isin(["EUR", "USD"]))
    .select(
        "id",
        "plan",
        F.rank().over(w).alias("tier_value_rank"),
        F.avg("tier_value").over(w2).alias("currency_avg_tier_value"),
        F.dense_rank().over(w3).alias("tier_value_dense_rank"),
    )
)

valid = (
    scored
    .filter(F.col("tier_value_rank") != 6)
    .select(
        "plan",
        "tier_value_rank",
        (F.col("tier_value_dense_rank") / F.nullif("currency_avg_tier_value", F.lit(0))).alias("tier_value_dense_rank_per_currency_avg_tier_value"),
    )
)

remaining_keys = (
    valid
    .select("plan")
    .subtract(
        df_price_tiers
        .filter(F.col("currency").isin(["USD", "EUR"]))
        .select("plan")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

valid_price_tiers = (
    verified
    .filter(F.col("tier_value_rank") != 5)
    .withColumn("plan_upper", F.upper("plan"))
    .withColumn("plan_length", F.length("plan"))
)

scored_price_tiers = (
    valid_price_tiers
    .select(
        "plan",
        "tier_value_rank",
        F.round((F.col("tier_value_rank") / F.sum("tier_value_rank").over(w4)), 4).alias("tier_value_rank_share"),
        F.dense_rank().over(w5).alias("tier_value_dense_rank_per_currency_avg_tier_value_dense_rank"),
    )
)

filtered = (
    scored_price_tiers
    .filter(F.col("plan").isin(["pro", "enterprise"]))
    .withColumn("plan_length", F.length("plan"))
    .withColumn("tier_value_dense_rank_per_currency_avg_tier_value_dense_rank_adjusted", F.round((F.col("tier_value_dense_rank_per_currency_avg_tier_value_dense_rank") * 100), 2))
)

result = (
    filtered
    .select(
        "plan",
        "tier_value_rank",
        "tier_value_rank_share",
        "tier_value_dense_rank_per_currency_avg_tier_value_dense_rank",
        "tier_value_dense_rank_per_currency_avg_tier_value_dense_rank_adjusted",
    )
    .orderBy(
        F.col("tier_value_rank_share").desc_nulls_first(),
        F.col("tier_value_dense_rank_per_currency_avg_tier_value_dense_rank").desc(),
    )
)
