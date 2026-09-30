from pyspark.sql import functions as F

remaining_keys = (
    df_promotions
    .select("promo_code")
    .subtract(
        df_promotions
        .filter(F.col("promo_code").isin(["VIP", "SPRING10", "BUNDLE"]))
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
    .rollup(
        F.when(F.col("discount_pct") >= 0.33, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.count_if(F.col("promo_code") == "WELCOME5").alias("welcome5_events"),
        F.sum(F.when(F.col("channel") == "store", F.col("discount_pct")).otherwise(0)).alias("store_discount_pct"),
    )
    .filter(F.col("store_discount_pct") >= 0)
    .orderBy(F.col("discount_pct_tier").desc())
)
