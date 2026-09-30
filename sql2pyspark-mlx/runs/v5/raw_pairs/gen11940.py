from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([8, 28, 34]))
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
    .filter(F.col("unique_products") > 3)
    .drop("unique_products")
    .orderBy("avg_quantity")
)
