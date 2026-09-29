from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("tier_value") >= 62, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
    )
    .agg(
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.sum(F.when(F.col("currency") == "THB", F.col("tier_value")).otherwise(0)).alias("thb_tier_value"),
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("tier_value_per_eur"),
        F.percentile_approx("tier_value", 0.9).alias("tier_value_p90"),
    )
    .orderBy(F.col("thb_tier_value").desc_nulls_last())
)
