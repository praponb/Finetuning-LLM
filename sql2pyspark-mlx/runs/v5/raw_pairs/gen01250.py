from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("quantity") > 6) & F.col("product_id").isin([21, 34])) | (F.col("quantity") <= 3))
    .groupBy(
        F.when(F.col("unit_price") >= 410, "high").when(F.col("unit_price") >= 90, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("unit_price").alias("unit_price_count"))
    .orderBy("unit_price_bucket", "unit_price_count", "product_id", "quantity_bucket")
    .limit(5)
)
