from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.month("effective_from").isin([2, 8, 9]))
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    parsed
    .groupBy("currency")
    .agg(
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
    )
    .orderBy("currency")
)
