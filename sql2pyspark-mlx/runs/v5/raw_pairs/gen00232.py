from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

filtered = (
    exploded
    .filter(F.col("effective_from") <= "2024-03-01")
    .withColumn("tier_value_whole", F.floor("tier_value"))
)

filtered_price_tiers = (
    filtered
    .filter(F.col("tier_value") < 41)
    .select(
        "effective_from",
        "currency",
        "tier_value_whole",
        (F.col("tier_value") + F.col("tier_value_whole")).alias("tier_value_plus_tier_value_whole"),
    )
)

valid = (
    filtered_price_tiers
    .filter((F.col("effective_from") > F.lit("2024-03-23").cast("date")) & (F.col("tier_value_plus_tier_value_whole") < 64))
    .select("*", F.substring("currency", 1, 2).alias("currency_prefix"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("tier_value_whole") >= 89, "PREMIUM").otherwise("STANDARD").alias("tier_value_whole_tier"),
    )
    .agg(
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("tier_value_plus_tier_value_whole", [0.25, 0.5, 0.75]).alias("tier_value_plus_tier_value_whole_quartiles"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("tier_value_whole")).otherwise(0)).alias("eur_tier_value_whole"),
    )
    .filter(F.col("eur_tier_value_whole") > 0)
    .orderBy("tier_value_whole_tier")
)
