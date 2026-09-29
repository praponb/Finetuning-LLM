from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter((F.col("o.channel") != "mobile") & (F.col("oi.unit_price") <= 160))
    .select(
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("oi.order_id"),
        F.col("oi.unit_price"),
    )
)
