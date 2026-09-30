from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("tier_value"),
    )
)

result = (
    exploded
    .groupBy(
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.when(F.col("tier_value") >= 67, "PREMIUM").otherwise("STANDARD").alias("tier_value_tier"),
    )
    .agg(
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("tier_value_per_usd"),
        F.sum("tier_value").alias("total_tier_value"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
    )
)
