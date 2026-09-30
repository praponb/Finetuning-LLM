from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("oi.unit_price") < 290)
    .select(F.col("o.status"), F.col("o.shipping_fee"), F.col("oi.unit_price"))
)
