from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .select(F.col("oi.id"), F.col("oi.product_id"), F.col("o.shipping_fee"))
    .orderBy(F.col("id").desc(), F.col("shipping_fee").desc())
)
