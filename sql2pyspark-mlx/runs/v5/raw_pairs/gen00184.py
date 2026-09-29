from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.filter(F.col("channel") == "web").alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left_semi")
    .filter(F.col("shipments.carrier").isin(["FedEx", "Flash", "UPS"]))
    .select(F.col("shipments.id"), F.col("shipments.status"))
)
