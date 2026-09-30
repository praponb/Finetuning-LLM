from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .select(
        F.col("order_items.id"),
        F.col("order_items.quantity"),
        F.col("order_items.product_id"),
        F.col("orders.customer_id"),
        F.col("orders.discount"),
        F.col("products.supplier_id"),
        F.col("products.stock_qty"),
    )
    .orderBy(F.col("stock_qty").desc(), F.col("discount").desc())
)
