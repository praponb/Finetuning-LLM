from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.min("cost").alias("min_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("sbo.avg_cost"),
        F.col("sbo.min_cost"),
    )
    .orderBy("shipping_fee")
)
