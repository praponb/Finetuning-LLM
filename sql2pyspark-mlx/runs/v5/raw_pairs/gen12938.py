from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") > 23) & (F.col("store_id") == 38))
    .agg(F.avg("shipping_fee").alias("avg_shipping_fee"))
)
