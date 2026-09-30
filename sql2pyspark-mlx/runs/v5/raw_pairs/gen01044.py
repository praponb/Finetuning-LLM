from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", F.year("order_date").alias("order_year"))
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .filter(F.col("total_shipping_fee") >= 220)
    .orderBy("total_shipping_fee", F.col("status").desc(), "order_year")
    .limit(1)
)
