from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 36)
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(F.round(F.stddev("discount"), 2).alias("stddev_discount"))
    .filter(F.col("stddev_discount") >= 35)
    .select("order_month", "stddev_discount")
    .orderBy("stddev_discount", F.col("order_month").desc())
)
