from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "left")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .select(
        F.col("order_items.unit_price"),
        F.col("products.product_name"),
        F.col("orders.store_id"),
        F.round(F.col("products.price"), 0).alias("price_rounded"),
    )
)
