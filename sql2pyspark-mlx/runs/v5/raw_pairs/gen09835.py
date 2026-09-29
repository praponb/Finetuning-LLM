from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "left_semi")
    .filter(F.col("orders.customer_id").isNull())
    .select(F.col("orders.id"), F.col("orders.channel"))
)
