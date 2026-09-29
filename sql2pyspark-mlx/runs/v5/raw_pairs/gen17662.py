from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.filter(F.col("carrier") == "Flash").alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "left_anti")
    .select(
        F.col("orders.id"),
        F.col("orders.customer_id"),
        F.col("orders.status"),
        F.col("orders.store_id"),
    )
)
