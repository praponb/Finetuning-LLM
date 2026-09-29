from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.channel").isin(["store", "partner", "mobile"]))
    .groupBy(F.col("orders.status"))
    .agg(F.round(F.stddev(F.col("shipments.cost")), 2).alias("stddev_cost"))
    .filter(F.col("stddev_cost") >= 28)
    .orderBy("status")
)
