from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "right")
    .select(
        F.col("shipments.shipped_date"),
        F.col("shipments.carrier"),
        F.col("orders.discount"),
        F.col("orders.status"),
        F.col("orders.amount"),
    )
    .orderBy(F.col("discount").desc())
)
