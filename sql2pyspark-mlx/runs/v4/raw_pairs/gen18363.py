from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter((F.col("orders.status") == "COMPLETED") & F.col("orders.status").isin(["CANCELLED", "PENDING"]))
    .select(
        F.col("shipments.id"),
        F.col("shipments.order_id"),
        F.col("shipments.status"),
        F.col("orders.shipping_fee"),
        F.length(F.col("shipments.carrier")).alias("carrier_length"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
