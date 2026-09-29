from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
    )
)
