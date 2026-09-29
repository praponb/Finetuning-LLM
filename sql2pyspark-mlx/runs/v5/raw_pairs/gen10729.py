from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("tier_value"))
)

result = (
    exploded
    .groupBy(
        F.when(F.col("tier_value") >= 84, "PREMIUM").otherwise("STANDARD").alias("tier_value_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.percentile_approx("tier_value", 0.95).alias("tier_value_p95"),
    )
    .orderBy(F.col("tier_value_p95").asc_nulls_first())
)
