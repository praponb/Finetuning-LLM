from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

cleaned = (
    lines
    .filter((F.col("effective_from") > F.lit("2024-03-04").cast("date")) & (F.col("tier_value") < 55))
    .select(
        "plan",
        "currency",
        F.when(F.col("currency").isin(["USD", "THB"]), "Core").otherwise("Other").alias("currency_group"),
        F.upper("currency").alias("currency_upper"),
    )
)

kept_keys = (
    cleaned
    .select("currency")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from") >= "2024-01-01")
        .select("currency")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

cleaned_price_tiers = (
    verified
    .filter(F.col("currency").isin(["THB", "USD"]))
    .select("*", F.lower("plan").alias("plan_lower"))
)

result = (
    cleaned_price_tiers
    .select("plan", "currency", "currency_group", "currency_upper", "plan_lower")
    .orderBy("plan", "currency")
)
