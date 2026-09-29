from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("o.shipping_fee").between(14, 26) & F.col("oi.product_id").isNotNull())
    .select(F.col("oi.quantity"), F.col("o.channel"))
)
