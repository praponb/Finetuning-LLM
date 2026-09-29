from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("effective_from").between("2023-06-26", "2024-08-01"))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("month", "effective_from").alias("activity_month"))
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.countDistinct("currency").alias("distinct_currencies"),
    )
    .orderBy("distinct_currencies")
)
