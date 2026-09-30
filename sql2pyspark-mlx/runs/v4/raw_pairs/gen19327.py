from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("plan") == "pro")
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("MONTH", "effective_from").alias("activity_month"))
    .agg(
        F.percentile_approx("entry_price", 0.5).alias("entry_price_p50"),
        F.sum(F.when(F.col("currency") == "USD", F.col("entry_price")).otherwise(0)).alias("usd_entry_price"),
        F.percentile_approx("entry_price", 0.9).alias("entry_price_p90"),
    )
    .orderBy(F.col("entry_price_p50").desc())
)
