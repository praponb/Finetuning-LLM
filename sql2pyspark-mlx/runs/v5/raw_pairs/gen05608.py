from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", "channel")
    .agg(F.max("amount").alias("max_amount"))
)
