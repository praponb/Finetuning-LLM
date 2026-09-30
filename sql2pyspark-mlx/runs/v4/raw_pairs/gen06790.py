from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("plan").isin(["enterprise", "pro", "basic"]))
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("lowest_price") >= 28, "LARGE").otherwise("SMALL").alias("lowest_price_tier"),
    )
    .agg(
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price"),
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price_2"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.max("lowest_price").alias("max_lowest_price"),
        F.sum(F.when(F.col("plan") == "starter", F.col("lowest_price")).otherwise(0)).alias("starter_lowest_price"),
        F.max(F.when(F.col("plan") == "basic", F.col("lowest_price"))).alias("basic_max_lowest_price"),
    )
    .orderBy(F.col("avg_lowest_price_2").asc_nulls_last())
)
