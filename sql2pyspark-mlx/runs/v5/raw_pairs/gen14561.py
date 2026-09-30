from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

result = (
    flattened
    .groupBy(
        "plan",
        F.when(F.col("element") >= 82, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
    )
    .orderBy(F.col("usd_rows").desc_nulls_last())
)
