from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter((F.col("orders.channel") == "store") & F.col("orders.discount").between(0.28, 0.37))
    .select(
        F.col("order_items.quantity"),
        F.col("orders.customer_id"),
        F.col("products.price"),
        F.col("products.product_name"),
    )
    .orderBy(F.col("quantity").desc())
)
