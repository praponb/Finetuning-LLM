from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.status"), F.quarter(F.col("o.order_date")).alias("order_quarter"))
    .agg(F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"))
    .filter(F.col("stddev_cost") > 54)
    .orderBy("status")
)
