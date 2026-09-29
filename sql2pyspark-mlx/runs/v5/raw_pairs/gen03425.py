from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("OrderTotals"), F.col("orders.id") == F.col("OrderTotals.order_id"), "left")
    .filter(F.col("OrderTotals.revenue") > 24)
    .select(
        F.col("orders.id"),
        F.col("orders.discount"),
        F.col("orders.amount"),
        F.col("OrderTotals.revenue"),
        F.col("OrderTotals.total_unit_price"),
    )
    .orderBy(F.col("total_unit_price").desc())
)
