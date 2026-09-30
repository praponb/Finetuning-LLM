from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

result = (
    exploded
    .groupBy(
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.when(F.col("tier_value") >= 68, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
    )
    .agg(
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
    )
)
