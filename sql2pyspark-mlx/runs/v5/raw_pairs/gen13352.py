from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.year("shipped_date") == 2023)
    .groupBy("order_id")
    .agg(
        F.min("cost").alias("min_cost"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("sbo.min_cost"),
        F.col("sbo.avg_cost"),
    )
    .orderBy("id")
)
