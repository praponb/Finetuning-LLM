from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .filter(F.col("sbo.cost_range") >= 70)
    .select(F.col("o.status"), F.col("sbo.cost_range"))
)
