from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("discounts").alias("tier_value"))
)

result = (
    exploded
    .groupBy(
        "plan",
        F.when(F.col("tier_value") >= 66, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
    )
    .agg(
        F.avg("tier_value").alias("avg_tier_value"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.min("tier_value").alias("min_tier_value"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value_2"),
    )
    .filter(F.col("avg_tier_value") > 0)
    .orderBy(F.col("avg_tier_value_2").desc())
)
