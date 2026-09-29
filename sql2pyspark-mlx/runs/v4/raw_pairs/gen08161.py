from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("order_id") == 7)
    .groupBy("order_id")
    .agg(F.max("shipped_date").alias("latest_shipped_date"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .select(
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("sbo.latest_shipped_date"),
    )
    .orderBy(F.col("customer_id").desc())
)
