from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

valid = (
    exploded
    .filter(F.col("currency").isin(["USD", "EUR"]))
    .withColumn("effective_from_month", F.month("effective_from"))
)

valid_price_tiers = (
    valid
    .filter(F.col("effective_from_month").isNotNull())
    .select("*", F.weekofyear("effective_from").alias("effective_from_weekofyear"))
)

valid_price_tiers_2 = (
    valid_price_tiers
    .filter(F.month("effective_from").isin([3, 4, 11]))
    .select(
        "id",
        "effective_from",
        "tier_value",
        "effective_from_month",
        (F.col("effective_from_month") * F.col("tier_value")).alias("effective_from_month_times_tier_value"),
    )
)

kept_keys = (
    valid_price_tiers_2
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.month("effective_from").isin([3, 5, 6]))
        .select("id")
    )
)

verified = (
    valid_price_tiers_2
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("tier_value") < 80)
    .withColumn("tier_value_minus_effective_from_month_times_tier_value", (F.col("tier_value") - F.col("effective_from_month_times_tier_value")))
)

report = (
    filtered
    .select(
        "id",
        "effective_from",
        "tier_value",
        "effective_from_month",
        "effective_from_month_times_tier_value",
        "tier_value_minus_effective_from_month_times_tier_value",
    )
)

result = (
    report
    .select(
        "id",
        "effective_from",
        "tier_value",
        "effective_from_month",
        "effective_from_month_times_tier_value",
        "tier_value_minus_effective_from_month_times_tier_value",
    )
    .orderBy("effective_from_month")
)
