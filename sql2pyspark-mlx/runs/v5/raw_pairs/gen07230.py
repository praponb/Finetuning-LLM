from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    exploded
    .groupBy("currency")
    .agg(
        F.sum(F.when(F.col("currency") == "THB", F.col("tier_value"))).alias("thb_total_tier_value"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value_2"),
    )
    .orderBy(F.col("thb_total_tier_value").asc_nulls_first())
)
