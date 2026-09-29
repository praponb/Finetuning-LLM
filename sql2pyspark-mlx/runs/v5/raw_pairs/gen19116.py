from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "RETURNED", "DELIVERED"]))
    .groupBy("order_id")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .select(F.col("o.discount"), F.col("ot.cost_range"))
)
