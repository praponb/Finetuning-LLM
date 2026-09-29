from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") <= 3) & F.col("product_id").isNotNull())
    .groupBy(
        F.when(F.col("unit_price") >= 320, "high").when(F.col("unit_price") >= 90, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.sum("quantity").alias("total_quantity"),
    )
)
