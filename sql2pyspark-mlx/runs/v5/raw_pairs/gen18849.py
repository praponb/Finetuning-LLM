from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .select(
        F.col("orders.customer_id"),
        F.col("shipments.delivered_date"),
        F.col("shipments.status"),
        F.col("shipments.carrier"),
    )
    .orderBy("carrier")
)
