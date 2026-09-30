from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("plan") != "basic")
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .groupBy("currency")
    .agg(
        F.percentile_approx("lowest_price", [0.25, 0.5, 0.75]).alias("lowest_price_quartiles"),
        F.round(F.sum("lowest_price") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("lowest_price_per_pro"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sum(F.when(F.col("currency") == "USD", F.col("lowest_price")).otherwise(0)).alias("usd_lowest_price"),
        F.percentile_approx("lowest_price", 0.5).alias("lowest_price_p50"),
        F.sum("lowest_price").alias("total_lowest_price"),
    )
    .filter(F.col("lowest_price_p50") > 0)
    .orderBy(F.col("total_lowest_price").desc())
)
