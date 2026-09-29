from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
    )
    .filter(F.col("avg_amount") >= 60)
    .orderBy("avg_amount", "stddev_shipping_fee")
)
