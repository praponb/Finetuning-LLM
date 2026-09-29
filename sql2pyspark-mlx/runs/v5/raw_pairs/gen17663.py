from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "right")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .filter(F.month(F.col("orders.order_date")).isin([2, 7, 9]))
    .select(
        F.col("shipments.id"),
        F.col("shipments.delivered_date"),
        F.col("orders.discount"),
        F.col("orders.status"),
        F.col("stores.region"),
        F.col("stores.sqft"),
    )
)
