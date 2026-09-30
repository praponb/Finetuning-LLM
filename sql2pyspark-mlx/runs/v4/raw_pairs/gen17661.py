from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .filter(F.col("order_id").isin([20, 25]))
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("product_stats"), F.col("products.id") == F.col("product_stats.product_id"), "inner")
    .filter(F.col("product_stats.avg_quantity") > 6.8)
    .select(
        F.col("products.id"),
        F.col("products.stock_qty"),
        F.col("products.product_name"),
        F.col("product_stats.avg_quantity"),
    )
)
