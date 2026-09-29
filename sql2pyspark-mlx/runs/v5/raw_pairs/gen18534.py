from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("store_id") == 3)
    .withColumn("greatest_shipping_fee_amount", F.greatest("shipping_fee", "amount"))
)

orders_stats = (
    base
    .groupBy("channel")
    .agg(F.count("shipping_fee").alias("shipping_fee_count"))
)

result = (
    orders_stats
    .select("channel", "shipping_fee_count")
    .orderBy("channel")
)
