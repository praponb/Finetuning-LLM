from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("entry_price") >= 36, "HIGH").otherwise("LOW").alias("entry_price_tier"),
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
    )
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.avg(F.when(F.col("plan") == "starter", F.col("entry_price"))).alias("starter_avg_entry_price"),
        F.avg(F.when(F.col("plan") == "basic", F.col("entry_price"))).alias("basic_avg_entry_price"),
        F.min("entry_price").alias("min_entry_price"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_rows"),
    )
    .filter(F.col("starter_avg_entry_price") > 0)
)
