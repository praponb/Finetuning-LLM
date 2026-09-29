from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .select(F.col("o.discount"), F.col("oi.unit_price"), F.col("oi.order_id"))
)
