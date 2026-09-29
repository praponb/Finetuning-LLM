from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("orders.status") == "CANCELLED")
    .select(
        F.col("shipments.id"),
        F.col("shipments.status"),
        F.col("shipments.carrier"),
        F.col("orders.store_id"),
        F.col("customers.name"),
        F.date_sub(F.col("shipments.delivered_date"), 47).alias("delivered_date_minus_47d"),
    )
)
