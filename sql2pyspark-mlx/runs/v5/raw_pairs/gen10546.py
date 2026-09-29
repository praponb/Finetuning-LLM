from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 27)
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.countDistinct("unit_price").alias("unique_products"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)
