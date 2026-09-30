from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.unit_price").isNotNull())
    .select(
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("oi.unit_price"),
    )
    .orderBy("shipping_fee")
)
