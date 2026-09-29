from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

result = (
    exploded
    .groupBy(
        F.when(F.col("product_id") >= 9, "LARGE").otherwise("SMALL").alias("product_id_tier"),
    )
    .agg(
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_rows"),
        F.min("qty").alias("min_qty"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("product_id"), 2).alias("avg_product_id"),
    )
    .filter(F.col("avg_product_id") >= 0)
    .orderBy("product_id_tier")
)
