from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("shipments.carrier").isin(["Flash", "Kerry", "DHL"]))
    .select(
        F.col("orders.amount"),
        F.col("orders.status"),
        F.col("orders.channel"),
        F.col("shipments.delivered_date"),
        F.col("shipments.status").alias("shipment_status"),
    )
    .orderBy("delivered_date", "status")
)
