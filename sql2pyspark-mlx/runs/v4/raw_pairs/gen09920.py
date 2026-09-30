from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("tier_value") >= 66, "HIGH").otherwise("LOW").alias("tier_value_tier"),
    )
    .agg(
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
        F.sum(F.when(F.col("currency") == "USD", F.col("tier_value")).otherwise(0)).alias("usd_tier_value"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("tier_value_per_pro"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
    )
)
