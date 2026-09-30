from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("order_summary"), F.col("orders.id") == F.col("order_summary.order_id"), "left")
    .filter(F.col("order_summary.avg_quantity") > 35)
    .select(
        F.col("orders.id"),
        F.col("orders.customer_id"),
        F.col("order_summary.avg_quantity"),
        F.col("order_summary.max_quantity"),
    )
    .orderBy(F.col("max_quantity").desc())
)
