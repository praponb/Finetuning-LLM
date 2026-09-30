from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") >= 960) | F.col("shipping_fee").isNotNull())
    .groupBy("status")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)
