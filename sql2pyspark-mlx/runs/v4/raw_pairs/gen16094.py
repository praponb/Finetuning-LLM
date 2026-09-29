from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("p.brand"),
        F.col("o.status"),
        F.col("o.discount"),
        F.round((F.col("oi.unit_price") / F.col("oi.quantity")), 2).alias("line_total"),
    )
)
