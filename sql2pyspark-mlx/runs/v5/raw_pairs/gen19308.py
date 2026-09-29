from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("amount") < 540))
    .select("*", F.length("status").alias("status_length"))
)

result = (
    base
    .groupBy("customer_id", "store_id")
    .pivot("status", ["PENDING", "RETURNED", "CANCELLED"])
    .agg(F.max("amount").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)
