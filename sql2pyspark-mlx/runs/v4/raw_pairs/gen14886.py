from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .select(
        F.col("shipments.shipped_date"),
        F.col("shipments.status"),
        F.col("shipments.carrier"),
        F.col("orders.amount"),
        F.col("orders.customer_id"),
        F.col("orders.store_id"),
        F.date_trunc("YEAR", F.col("shipments.delivered_date")).alias("delivered_date_period"),
    )
)
