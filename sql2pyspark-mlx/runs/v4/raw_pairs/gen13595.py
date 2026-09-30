from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("tier_value"))

valid = (
    lines
    .filter(F.col("tier_value") >= 46)
    .select(
        "*",
        F.when(F.col("tier_value") >= 73, "High").when(F.col("tier_value") >= 29, "Medium").otherwise("Low").alias("tier_value_level"),
    )
)

filtered = (
    valid
    .filter(F.col("tier_value") > 44)
    .select("*", F.initcap(F.trim("plan")).alias("plan_title"))
)

kept_keys = (
    filtered
    .select("id")
    .exceptAll(
        df_price_tiers
        .filter(F.month("effective_from").isin([1, 7, 12]))
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("plan", "tier_value")
    .orderBy(F.col("tier_value").desc_nulls_last())
)
