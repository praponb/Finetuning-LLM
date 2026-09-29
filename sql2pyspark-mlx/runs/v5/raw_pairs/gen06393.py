from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 16)
    .groupBy("channel")
    .agg(
        F.min("discount").alias("min_discount"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
    )
)
