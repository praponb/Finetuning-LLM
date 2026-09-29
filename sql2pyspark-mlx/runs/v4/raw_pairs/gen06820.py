from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.col("products.product_name").isNotNull() & (F.lower(F.trim(F.col("products.product_name"))) == "running shoes"))
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("orders.order_date"),
        F.col("products.brand"),
        (F.col("order_items.unit_price") * F.col("order_items.quantity")).alias("line_total"),
    )
)
