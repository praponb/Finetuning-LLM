from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_limits",
        "tier_prices",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .groupBy("plan")
    .agg(
        F.percentile_approx("entry_price", 0.5).alias("entry_price_p50"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.percentile_approx("entry_price", 0.5).alias("entry_price_p50_2"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("plan").desc_nulls_last())
)
