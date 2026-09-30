from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("promo_code")
    .agg(
        F.percentile_approx("discount_pct", 0.5).alias("discount_pct_p50"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.sum(F.when(F.col("promo_code") == "WELCOME5", F.col("discount_pct"))).alias("welcome5_total_discount_pct"),
    )
)
