from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") >= 32) & (F.col("amount") < 1530))
    .groupBy("status", F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(F.countDistinct("amount").alias("unique_customers"))
)
