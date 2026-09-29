from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter((F.length(F.col("p.product_name")) >= 6) & (F.col("oi.quantity") != 7))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("p.category"),
    )
)
