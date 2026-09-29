from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .pivot("status", ["SHIPPED", "CANCELLED"])
    .agg(F.avg("amount").alias("value_sum"))
)
