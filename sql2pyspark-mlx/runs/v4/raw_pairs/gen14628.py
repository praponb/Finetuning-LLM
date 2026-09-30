from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .filter(F.col("order_items.product_id").isin([7, 26, 28, 34]))
    .select(
        F.col("order_items.id"),
        F.col("order_items.quantity"),
        F.col("products.product_name"),
        F.col("products.supplier_id"),
        F.col("orders.order_date"),
    )
)
