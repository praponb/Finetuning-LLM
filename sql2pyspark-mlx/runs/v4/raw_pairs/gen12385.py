from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("customer_id")
    .pivot("channel", ["web", "store"])
    .agg(F.avg("amount").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
