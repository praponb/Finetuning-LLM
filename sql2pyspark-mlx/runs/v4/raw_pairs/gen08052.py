from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.14)
    .select("store_id", "customer_id", F.lower("status").alias("status_lower"))
)
