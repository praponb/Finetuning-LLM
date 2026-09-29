from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode("discounts").alias("tier_value"))
)

result = (
    exploded
    .groupBy(
        F.when(F.col("tier_value") >= 71, "PREMIUM").otherwise("STANDARD").alias("tier_value_tier"),
    )
    .agg(
        F.avg("tier_value").alias("avg_tier_value"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("tier_value", 0.5).alias("tier_value_p50"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("avg_tier_value") > 0)
    .orderBy(F.col("tier_value_tier").desc_nulls_first())
)
