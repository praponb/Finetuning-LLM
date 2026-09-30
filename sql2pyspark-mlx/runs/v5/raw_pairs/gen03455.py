from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(F.count("*").alias("num_shipments"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .filter(F.col("sbo.num_shipments") >= 9)
    .select(F.col("o.customer_id"), F.col("sbo.num_shipments"))
    .orderBy(F.col("num_shipments").desc())
)
