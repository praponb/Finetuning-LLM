from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 12) & (F.col("store_id").isNotNull() | (F.col("discount") >= 0.36)))
    .groupBy("status", "channel")
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
)
