from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("element"))
)

result = (
    exploded
    .groupBy(
        F.when(F.col("element") >= 75, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
        "plan",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("plan") == "starter", F.col("element")).otherwise(0)).alias("starter_element"),
    )
    .orderBy(F.col("element_tier").desc())
)
