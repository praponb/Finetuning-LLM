from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .filter(F.col("products.launch_date").between("2023-12-24", "2024-12-01"))
    .select(
        F.col("order_items.order_id"),
        F.col("orders.amount"),
        F.col("orders.customer_id"),
        F.col("products.product_name"),
        F.col("products.launch_date"),
    )
)
