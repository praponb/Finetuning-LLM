from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .groupBy(F.col("o.status"))
    .agg(F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"))
    .orderBy("status", F.col("stddev_quantity").desc())
)
