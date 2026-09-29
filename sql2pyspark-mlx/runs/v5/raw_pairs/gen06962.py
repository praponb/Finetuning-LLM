from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 1540)
    .groupBy("store_id")
    .agg(F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"))
)
