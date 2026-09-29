from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("shipped_date") < "2025-02-27")
    .groupBy("order_id")
    .agg(F.count("cost").alias("cost_count"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .select(F.col("o.discount"), F.col("o.shipping_fee"), F.col("sbo.cost_count"))
    .orderBy(F.col("discount").desc())
)
