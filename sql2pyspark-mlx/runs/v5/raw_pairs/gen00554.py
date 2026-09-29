from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("currency").isin(["EUR", "USD", "THB"]))
    .select(
        "id",
        "currency",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .withColumn("lowest_price_tier", F.when(F.col("lowest_price") >= 34, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["lowest_price_tier", "currency"], ["lowest_price_tier"]],
        "lowest_price_tier",
        "currency",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.countDistinct("currency").alias("distinct_currencies_2"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("distinct_ids")
)
