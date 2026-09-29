from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    exploded
    .groupBy(
        "customer_id",
        F.when(F.col("qty") >= 5, "HIGH").otherwise("LOW").alias("qty_tier"),
    )
    .agg(
        F.min("price").alias("min_price"),
        F.count_if(F.col("promo") == "SPRING10").alias("spring10_events"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.sum(F.when(F.col("promo") == "SPRING10", F.col("price")).otherwise(0)).alias("spring10_price"),
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_rows"),
    )
    .orderBy(F.col("bundle_rows").asc_nulls_first())
)
