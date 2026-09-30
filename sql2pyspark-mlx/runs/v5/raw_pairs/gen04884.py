from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("entry_price") >= 27, "HIGH").otherwise("LOW").alias("entry_price_tier"),
    )
    .agg(
        F.sum("entry_price").alias("total_entry_price"),
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
    )
    .orderBy(F.col("total_entry_price").desc_nulls_last())
)
