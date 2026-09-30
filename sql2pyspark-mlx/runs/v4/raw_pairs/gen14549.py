from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    df_products.alias("products")
    .join(order_items_by_product.alias("t"), F.col("products.id") == F.col("t.product_id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.stock_qty"),
        F.col("t.revenue"),
        F.col("t.avg_unit_price"),
    )
)
