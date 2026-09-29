from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .select(
        F.col("shipments.id"),
        F.col("shipments.shipped_date"),
        F.col("shipments.delivered_date"),
        F.col("shipments.status"),
        F.col("orders.status").alias("order_status"),
        F.col("orders.store_id"),
        F.concat(F.col("shipments.carrier"), F.lit("/"), F.col("shipments.status")).alias("carrier_status"),
    )
    .orderBy(F.col("id").desc(), F.col("shipped_date").desc())
)
