from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") > 71)
    .orderBy("amount_range")
)
