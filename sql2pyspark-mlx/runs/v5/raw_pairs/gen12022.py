from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .groupBy(F.col("products.brand"))
    .agg(
        F.min(F.col("order_items.unit_price")).alias("min_unit_price"),
        F.min(F.col("order_items.quantity")).alias("min_quantity"),
    )
    .orderBy(F.col("brand").desc(), "min_unit_price")
)
