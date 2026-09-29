from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.product_id").isin([25, 39]))
    .select(
        F.col("oi.order_id"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("o.amount"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
    )
)
