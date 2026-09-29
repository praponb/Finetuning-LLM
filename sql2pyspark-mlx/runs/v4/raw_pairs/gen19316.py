from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 10)
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .pivot("channel", ["mobile", "partner", "store", "web"])
    .agg(F.avg("shipping_fee"))
)
