from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isin([10, 23, 24]))
    .select(F.col("o.shipping_fee"), F.col("o.channel"), F.col("s.cost"))
    .orderBy("channel")
)
