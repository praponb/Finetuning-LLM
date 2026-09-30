from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.order_date") > "2024-06-01")
    .select(
        F.col("shipments.shipped_date"),
        F.col("shipments.delivered_date"),
        F.col("orders.status"),
        F.col("orders.shipping_fee"),
        F.col("orders.order_date"),
    )
    .orderBy(F.col("delivered_date").desc(), "order_date")
)
