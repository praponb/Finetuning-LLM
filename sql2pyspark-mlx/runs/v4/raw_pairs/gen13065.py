from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .filter(F.col("oi.unit_price").isNotNull())
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("p.category"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
    )
)
