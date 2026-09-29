from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 4)
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 310, "high").when(F.col("unit_price") >= 70, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .filter(F.col("unique_products") >= 9)
    .select("product_id", "unit_price_bucket", "unique_products")
)
