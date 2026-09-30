from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("tier_value"))
)

result = (
    flattened
    .groupBy(
        F.when(F.col("tier_value") >= 62, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
    )
    .agg(
        F.sum(F.when(F.col("currency") == "EUR", F.col("tier_value")).otherwise(0)).alias("eur_tier_value"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.sum(F.when(F.col("currency") == "THB", F.col("tier_value"))).alias("thb_total_tier_value"),
        F.percentile_approx("tier_value", 0.9).alias("tier_value_p90"),
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("tier_value_per_basic"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
    )
    .orderBy("thb_total_tier_value")
)
