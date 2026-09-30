from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "inner")
    .filter(F.col("order_items.order_id").isin([33, 36, 37, 40]))
    .select(
        F.col("order_items.id"),
        F.col("order_items.order_id"),
        F.col("order_items.unit_price"),
        F.col("orders.channel"),
        F.col("products.category"),
        F.col("products.stock_qty"),
    )
)
