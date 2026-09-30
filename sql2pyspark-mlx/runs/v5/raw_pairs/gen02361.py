from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.filter(F.col("shipping_fee").between(10, 38)).alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left_semi")
    .select(
        F.col("shipments.id"),
        F.col("shipments.cost"),
        F.col("shipments.shipped_date"),
        F.col("shipments.delivered_date"),
    )
)
