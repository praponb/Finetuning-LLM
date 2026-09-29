from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.store_id") == 22)
    .select(
        F.col("orders.discount"),
        F.col("orders.customer_id"),
        F.col("orders.order_date"),
        F.col("shipments.cost"),
        F.col("shipments.order_id"),
    )
    .orderBy("cost")
)
