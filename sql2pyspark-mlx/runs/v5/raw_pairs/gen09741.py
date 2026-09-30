from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

result = (
    flattened
    .groupBy(
        F.when(F.col("price") >= 188, "LARGE").otherwise("SMALL").alias("price_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_count"),
        F.round(F.sum("qty") / F.nullif(F.count(F.when(F.col("promo") == "WELCOME5", 1)), F.lit(0)), 2).alias("qty_per_welcome5"),
    )
    .orderBy("price_tier")
)
