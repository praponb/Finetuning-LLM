from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .filter(F.col("orders.customer_id").isin([15, 16, 23]))
    .select(
        F.col("order_items.id"),
        F.col("order_items.product_id"),
        F.col("products.brand"),
        F.col("products.product_name"),
        F.col("orders.discount"),
        F.col("orders.channel"),
    )
)
