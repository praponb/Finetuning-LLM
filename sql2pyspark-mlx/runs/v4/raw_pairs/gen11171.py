from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "right")
    .filter(F.col("orders.shipping_fee").between(35, 37))
    .select(
        F.col("shipments.id"),
        F.col("shipments.order_id"),
        F.col("orders.channel"),
        F.col("orders.discount"),
        F.col("orders.store_id"),
        F.initcap(F.trim(F.col("shipments.status"))).alias("status_title"),
    )
    .orderBy(F.col("channel").desc())
)
