from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"))
    .agg(F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") > 32)
)
