from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .select(F.col("orders.order_date"), F.col("shipments.cost"))
    .orderBy("order_date")
)
