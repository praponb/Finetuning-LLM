from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("promo_code")
    .subtract(
        df_promotions
        .filter(F.col("ends_on") >= "2025-08-01")
        .select("promo_code")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        "promo_code",
        F.when(F.col("discount_pct") >= 0.29, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
    )
    .agg(
        F.count_if(F.col("promo_code") == "SPRING10").alias("spring10_events"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "BUNDLE", 1)), F.lit(0)), 2).alias("discount_pct_per_bundle"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("spring10_events") > 1)
    .orderBy(F.col("discount_pct_per_bundle").desc())
)
