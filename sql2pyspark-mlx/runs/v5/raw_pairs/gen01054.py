from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .groupBy(F.col("o.store_id"))
    .agg(F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"))
)
