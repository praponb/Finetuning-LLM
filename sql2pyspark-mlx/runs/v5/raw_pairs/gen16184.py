from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(F.max("shipped_date").alias("last_shipped_date"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("S"), F.col("o.id") == F.col("S.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("S.last_shipped_date"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
