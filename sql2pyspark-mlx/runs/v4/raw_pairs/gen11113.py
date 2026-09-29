from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("tier_value"))
)

cleaned = (
    flattened
    .filter((F.col("effective_from") > F.lit("2024-06-03").cast("date")) & F.col("plan").isin(["enterprise", "pro", "starter"]))
    .select(
        "*",
        F.when(F.col("tier_value") >= 61, "High").when(F.col("tier_value") >= 22, "Medium").otherwise("Low").alias("tier_value_level"),
    )
)

remaining_keys = (
    cleaned
    .select("plan", "id")
    .subtract(
        df_price_tiers
        .filter(F.col("currency").isin(["USD", "THB", "EUR"]))
        .select("plan", "id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = reconciled.select("plan", "tier_value", "tier_value_level")
