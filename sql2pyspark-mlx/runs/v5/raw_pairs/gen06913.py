from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("product_id").isin([3, 13, 15, 20])).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_semi")
    .select(F.col("o.id"), F.col("o.shipping_fee"), F.col("o.channel"))
    .orderBy("channel", "id")
    .limit(1)
)
