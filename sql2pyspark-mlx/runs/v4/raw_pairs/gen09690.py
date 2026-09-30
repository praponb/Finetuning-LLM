from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("order_summary"), F.col("orders.id") == F.col("order_summary.order_id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("order_summary.cost_range"),
    )
    .orderBy("order_date", "id")
    .limit(50)
)
