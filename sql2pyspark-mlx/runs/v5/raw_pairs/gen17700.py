from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") > 860) & F.month("order_date").isin([1, 3, 11]))
    .groupBy("status")
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .filter(F.col("total_shipping_fee") < 90)
)
