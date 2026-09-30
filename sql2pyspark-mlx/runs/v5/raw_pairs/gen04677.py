from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("effective_from") >= "2024-10-05")
    .select(
        "id",
        "effective_from",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .withColumn("year_start", F.date_trunc("YEAR", "effective_from"))
    .withColumn("week_start", F.date_trunc("WEEK", "effective_from"))
    .groupingSets(
        [["year_start", "week_start"], ["year_start"], []],
        "year_start",
        "week_start",
    )
    .agg(
        F.percentile_approx("entry_price", 0.5).alias("entry_price_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(F.col("distinct_ids_2").desc())
)
