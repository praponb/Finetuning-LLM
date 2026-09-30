from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.filter(F.col("status") != "RETURNED").alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left_semi")
    .filter(F.col("shipments.delivered_date") <= "2025-07-22")
    .select(F.col("shipments.id"), F.col("shipments.carrier"))
    .orderBy(F.col("id").desc())
)
