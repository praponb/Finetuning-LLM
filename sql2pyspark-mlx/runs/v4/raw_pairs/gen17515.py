from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(F.col("store_id").isin([1, 15, 21, 33]) & (F.col("customer_id").isin([9, 22, 32, 33]) | (F.col("shipping_fee") <= 19)))
)

agg = (
    valid_orders
    .groupBy("channel")
    .agg(F.count("discount").alias("discount_count"))
)

result = (
    agg
    .filter(F.col("discount_count") < 8)
    .orderBy("channel")
    .limit(3)
)
