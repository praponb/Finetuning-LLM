from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("promo_code")
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_rows"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
    )
    .orderBy(F.col("promo_code").desc())
)
