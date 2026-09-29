from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .groupBy("plan")
    .agg(
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.round(F.sum("lowest_price") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("lowest_price_per_enterprise"),
    )
    .filter(F.col("avg_lowest_price") < 0)
)
