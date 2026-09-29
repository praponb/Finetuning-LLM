from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("shipments.delivered_date") < "2023-05-01")
    .select(
        F.col("shipments.status"),
        F.col("shipments.order_id"),
        F.col("shipments.carrier"),
        F.col("orders.order_date"),
        F.col("orders.discount"),
        F.col("orders.store_id"),
    )
)
