from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-08-01", "2025-09-01"))
    .groupBy("order_id")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("t"), F.col("orders.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.stddev_cost") >= 77)
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("orders.customer_id"),
        F.col("t.stddev_cost"),
    )
)
