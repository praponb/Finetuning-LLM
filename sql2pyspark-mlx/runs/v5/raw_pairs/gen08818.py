from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    flattened
    .groupingSets([["currency", "plan"], ["currency"], []], "currency", "plan")
    .agg(
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("tier_value_per_thb"),
        F.percentile_approx("tier_value", 0.5).alias("tier_value_p50"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
)
