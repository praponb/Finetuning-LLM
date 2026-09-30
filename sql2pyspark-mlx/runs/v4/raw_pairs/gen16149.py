from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("cost") >= 76)
    .groupBy("order_id")
    .agg(
        F.min("cost").alias("min_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.avg_cost") >= 40)
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("t.min_cost"),
        F.col("t.avg_cost"),
    )
    .orderBy("min_cost", "id")
)
