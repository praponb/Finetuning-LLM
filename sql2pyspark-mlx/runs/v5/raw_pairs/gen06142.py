from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", F.explode_outer("tier_limits").alias("tier_value"))

base = (
    exploded
    .filter(F.col("tier_value") > 67)
    .select("*", F.ceil("tier_value").alias("tier_value_whole"))
)

matched_keys = (
    base
    .select("id")
    .intersectAll(
        df_price_tiers
        .filter(F.month("effective_from").isin([7, 8, 12]))
        .select("id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("tier_value") == 71)
    .select(
        "*",
        (F.col("tier_value") + F.col("tier_value_whole")).alias("tier_value_plus_tier_value_whole"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("tier_value_whole") >= 85, "PREMIUM").otherwise("STANDARD").alias("tier_value_whole_tier"),
    )
    .agg(
        F.round(F.avg("tier_value_whole"), 2).alias("avg_tier_value_whole"),
        F.percentile_approx("tier_value_whole", [0.25, 0.5, 0.75]).alias("tier_value_whole_quartiles"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
    )
)
