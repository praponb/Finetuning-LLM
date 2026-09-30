from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("shipments.order_id") == 39)
    .select(F.col("shipments.cost"), F.col("orders.shipping_fee"))
)
