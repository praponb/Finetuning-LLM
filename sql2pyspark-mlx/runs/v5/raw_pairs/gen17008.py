from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("currency").isin(["THB", "USD"]))
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_prices",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    unpacked
    .withColumn("activity_month", F.date_trunc("month", "effective_from"))
    .cube("currency", "activity_month")
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.grouping_id().alias("grouping_id"),
    )
)
