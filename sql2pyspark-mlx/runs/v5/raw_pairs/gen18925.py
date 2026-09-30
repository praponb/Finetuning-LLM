from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"))
)
