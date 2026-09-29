from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        F.when(F.col("discount_pct") >= 0.36, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("discount_pct_per_welcome5"),
        F.percentile_approx("discount_pct", 0.5).alias("discount_pct_p50"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
    .filter(F.col("discount_pct_per_welcome5") < 1000000)
)
