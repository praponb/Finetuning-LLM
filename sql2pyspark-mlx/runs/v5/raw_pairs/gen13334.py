from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = (
    flattened
    .rollup("currency")
    .agg(
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("tier_value_per_starter"),
        F.avg(F.when(F.col("currency") == "EUR", F.col("tier_value"))).alias("eur_avg_tier_value"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
    .filter(F.col("eur_avg_tier_value") < 1000000)
)
