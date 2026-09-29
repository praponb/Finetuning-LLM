from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.max("order_date").alias("latest_order_date"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
)
