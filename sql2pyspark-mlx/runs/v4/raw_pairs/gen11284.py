from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2024-02-01", "2024-07-28"))
    .groupBy("channel")
    .agg(F.avg("shipping_fee").alias("avg_shipping_fee"))
)
