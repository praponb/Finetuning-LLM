from pyspark.sql import functions as F

remaining_keys = (
    df_promotions
    .select("promo_code")
    .exceptAll(
        df_promotions
        .filter(F.col("discount_pct") >= 0.21)
        .select("promo_code")
    )
)

verified = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.date_trunc("MONTH", "ends_on").alias("activity_month"),
        F.when(F.col("discount_pct") >= 0.27, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.sum(F.when(F.col("promo_code") == "BUNDLE", F.col("discount_pct")).otherwise(0)).alias("bundle_discount_pct"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
    )
    .orderBy(F.col("activity_month").desc())
)
