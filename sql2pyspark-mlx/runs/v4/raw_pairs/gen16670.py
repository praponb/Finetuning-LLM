from pyspark.sql import functions as F

base = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.round((F.col("discount_pct") * 0.9), 2).alias("discount_pct_scaled"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

result = (
    base
    .withColumn("discount_pct_scaled_tier", F.when(F.col("discount_pct_scaled") >= 81, "HIGH").otherwise("LOW"))
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.27, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["discount_pct_scaled_tier", "discount_pct_tier"], ["discount_pct_scaled_tier"]],
        "discount_pct_scaled_tier",
        "discount_pct_tier",
    )
    .agg(
        F.round(F.sum("discount_pct_scaled") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("discount_pct_scaled_per_web"),
        F.avg("discount_pct_scaled").alias("avg_discount_pct_scaled"),
    )
    .orderBy(F.col("discount_pct_tier").desc())
)
