from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(
        F.when(F.col("discount_pct") >= 0.26, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
        "promo_code",
    )
    .agg(
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "WELCOME5", 1)), F.lit(0)), 2).alias("discount_pct_per_welcome5"),
    )
    .filter(F.col("discount_pct_per_welcome5") > 0)
    .orderBy(F.col("discount_pct_per_welcome5").desc_nulls_last())
)
