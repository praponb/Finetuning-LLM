from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") >= 950)
    .orderBy(F.col("stddev_shipping_fee").desc())
)
