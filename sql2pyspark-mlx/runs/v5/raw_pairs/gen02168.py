from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(F.countDistinct("cost").alias("unique_statuss"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "left")
    .filter(F.col("sbo.unique_statuss") > 4)
    .select(F.col("o.amount"), F.col("sbo.unique_statuss"))
)
