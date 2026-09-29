from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("unit_price") > 250).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_semi")
    .filter(F.col("o.customer_id").isin([4, 14, 23]))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
    )
)
