from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.avg("discount").alias("avg_discount"),
    )
)
