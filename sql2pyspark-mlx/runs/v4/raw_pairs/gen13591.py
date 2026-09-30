from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "right")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.col("orders.discount") >= 0.24)
    .select(
        F.col("order_items.id"),
        F.col("order_items.quantity"),
        F.col("order_items.order_id"),
        F.col("orders.discount"),
        F.col("orders.customer_id"),
        F.col("products.supplier_id"),
    )
    .orderBy(F.col("customer_id").desc())
)
