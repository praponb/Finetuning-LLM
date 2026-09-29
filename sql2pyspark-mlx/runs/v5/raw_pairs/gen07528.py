from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .select(
        F.col("shipments.id"),
        F.col("shipments.cost"),
        F.col("shipments.status"),
        F.col("orders.amount"),
        F.col("orders.status").alias("order_status"),
    )
)
