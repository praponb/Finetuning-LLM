from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

result = (
    lines
    .groupingSets([["promo", "customer_id"], ["promo"]], "promo", "customer_id")
    .agg(
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_count"),
        F.percentile_approx("qty", [0.25, 0.5, 0.75]).alias("qty_quartiles"),
        F.max(F.when(F.col("promo") == "SPRING10", F.col("qty"))).alias("spring10_max_qty"),
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("promo") == "VIP", 1)), F.lit(0)), 2).alias("price_per_vip"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_rows"),
    )
    .orderBy("customer_id")
)
