from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .filter(F.col("products.cost") >= 600)
    .select(
        F.col("order_items.order_id"),
        F.col("orders.customer_id"),
        F.col("orders.amount"),
        F.col("products.launch_date"),
    )
)
