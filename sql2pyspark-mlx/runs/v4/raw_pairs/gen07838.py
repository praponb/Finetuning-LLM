from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([5, 19, 24, 36]) & (F.col("amount") <= 1390) & (F.col("amount") >= 890))
    .groupBy("channel", F.year("order_date").alias("order_year"), "customer_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") < 52)
)
