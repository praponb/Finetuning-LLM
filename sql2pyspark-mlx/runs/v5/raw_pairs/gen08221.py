from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_limits").alias("tier_value"))
)

result = (
    flattened
    .groupBy("currency")
    .agg(
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
        F.percentile_approx("tier_value", 0.9).alias("tier_value_p90"),
        F.percentile_approx("tier_value", 0.9).alias("tier_value_p90_2"),
        F.sum("tier_value").alias("total_tier_value"),
        F.sum(F.when(F.col("currency") == "THB", F.col("tier_value")).otherwise(0)).alias("thb_tier_value"),
    )
    .orderBy("avg_tier_value")
)
