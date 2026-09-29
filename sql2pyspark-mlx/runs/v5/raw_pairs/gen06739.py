from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .select(
        F.col("order_items.id"),
        F.col("order_items.quantity"),
        F.col("order_items.product_id"),
        F.col("products.category"),
        F.col("products.price"),
        F.col("orders.customer_id"),
        F.when(F.col("products.price") >= 980, "High").when(F.col("products.price") >= 410, "Medium").otherwise("Low").alias("price_level"),
    )
)
