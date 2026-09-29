from pyspark.sql import functions as F

order_items_by_product = (
    df_order_items
    .groupBy("product_id")
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)

result = (
    df_products.alias("p")
    .join(order_items_by_product.alias("oibp"), F.col("p.id") == F.col("oibp.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
        F.col("oibp.revenue"),
    )
    .orderBy("stock_qty", "id")
    .limit(10)
)
