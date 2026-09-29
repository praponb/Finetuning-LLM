from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("tier_value"))
)

result = (
    lines
    .groupBy(
        F.when(F.col("tier_value") >= 69, "PREMIUM").otherwise("STANDARD").alias("tier_value_tier"),
    )
    .agg(
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.max(F.when(F.col("plan") == "enterprise", F.col("tier_value"))).alias("enterprise_max_tier_value"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("tier_value_per_usd"),
        F.percentile_approx("tier_value", 0.9).alias("tier_value_p90"),
    )
    .filter(F.col("tier_value_per_usd") >= 0)
)
