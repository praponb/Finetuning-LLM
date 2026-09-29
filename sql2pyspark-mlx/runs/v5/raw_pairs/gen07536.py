from pyspark.sql import functions as F

flattened = df_carts.select("id", "coupon_codes", F.inline("items"))

result = (
    flattened
    .groupBy(
        F.when(F.col("price") >= 209, "LARGE").otherwise("SMALL").alias("price_tier"),
        "promo",
        F.when(F.col("qty") >= 4, "HIGH").otherwise("LOW").alias("qty_tier"),
    )
    .agg(
        F.countDistinct("promo").alias("distinct_promos"),
        F.max(F.when(F.col("promo") == "BUNDLE", F.col("product_id"))).alias("bundle_max_product_id"),
    )
    .filter(F.col("bundle_max_product_id") > 0)
)
