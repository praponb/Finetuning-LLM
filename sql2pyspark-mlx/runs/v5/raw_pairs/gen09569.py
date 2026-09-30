from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_limits").alias("tier_value"))
)

filtered = (
    exploded
    .filter(F.col("tier_value") <= 62)
    .withColumn("tier_value_rounded", F.round("tier_value", 1))
)

filtered_price_tiers = (
    filtered
    .filter(F.col("tier_value_rounded") >= 76)
    .select(
        "*",
        F.when(F.col("plan").isin(["starter", "enterprise"]), "Group A").otherwise("Other").alias("plan_group"),
    )
)

matched_keys = (
    filtered_price_tiers
    .select("id", "plan")
    .intersectAll(
        df_price_tiers
        .filter(F.col("effective_from") > "2023-07-01")
        .select("id", "plan")
    )
)

verified = (
    filtered_price_tiers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("plan")
    .agg(
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.round(F.sum("tier_value_rounded") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("tier_value_rounded_per_starter"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles_2"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
    )
    .orderBy(F.col("enterprise_rows").asc_nulls_first())
)
