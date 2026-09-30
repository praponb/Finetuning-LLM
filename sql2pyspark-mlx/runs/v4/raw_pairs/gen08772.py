from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("p.stock_qty"),
    )
    .orderBy(F.col("unit_price").desc())
)
