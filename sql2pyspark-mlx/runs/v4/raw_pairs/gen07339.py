from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("store_id") == 18) & (F.col("discount") <= 0.38) & (F.year("order_date") == 2025))
    .select("customer_id", F.ceil("shipping_fee").alias("shipping_fee_whole"))
)
