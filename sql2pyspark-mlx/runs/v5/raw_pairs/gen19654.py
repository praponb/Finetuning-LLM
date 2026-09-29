from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
    .orderBy("total_shipping_fee")
)
