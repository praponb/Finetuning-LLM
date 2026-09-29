from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("cost") <= 47).alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .filter(F.col("o.customer_id").isin([4, 12, 25, 32]))
    .select(F.col("o.id"), F.col("o.amount"), F.col("o.shipping_fee"))
)
