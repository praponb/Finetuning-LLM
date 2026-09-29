from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "outer")
    .filter(F.col("shipments.delivered_date") >= "2023-06-01")
    .select(
        F.col("shipments.order_id"),
        F.col("shipments.status"),
        F.col("shipments.delivered_date"),
        F.col("orders.amount"),
        F.col("orders.channel"),
    )
    .orderBy(F.col("order_id").desc())
)
