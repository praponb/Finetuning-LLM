from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.month(F.col("o.order_date")).alias("order_month"))
    .agg(F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"))
    .orderBy(F.col("stddev_amount").desc())
)
