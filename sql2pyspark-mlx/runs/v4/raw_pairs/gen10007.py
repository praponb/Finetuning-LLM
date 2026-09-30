from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        F.when(F.col("discount_pct") >= 0.3, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
        "promo_code",
    )
    .agg(
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("discount_pct_per_store"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("distinct_promo_codes").desc_nulls_first())
)
