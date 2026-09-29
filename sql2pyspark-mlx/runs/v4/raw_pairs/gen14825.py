from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"))
)
