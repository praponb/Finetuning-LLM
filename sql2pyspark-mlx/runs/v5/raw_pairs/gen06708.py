from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", F.explode("discounts").alias("tier_value"))

matched_keys = (
    flattened
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.month("effective_from").isin([2, 3, 9]))
        .select("id")
    )
)

verified = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("tier_value").between(45, 60))
    .select("*", F.ceil("tier_value").alias("tier_value_whole"))
)

valid = (
    filtered
    .filter(F.col("tier_value") >= 80)
    .withColumn("adjusted_tier_value", F.round((F.col("tier_value") * 100), 2))
)

result = (
    valid
    .groupBy(
        F.when(F.col("adjusted_tier_value") >= 89, "HIGH").otherwise("LOW").alias("adjusted_tier_value_tier"),
    )
    .agg(
        F.percentile_approx("tier_value_whole", [0.25, 0.5, 0.75]).alias("tier_value_whole_quartiles"),
        F.sum("adjusted_tier_value").alias("total_adjusted_tier_value"),
        F.percentile_approx("tier_value_whole", 0.95).alias("tier_value_whole_p95"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("tier_value_whole_p95")
)
