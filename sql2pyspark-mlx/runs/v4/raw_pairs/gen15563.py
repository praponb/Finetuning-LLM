from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select("id", "store_id", F.substring("status", 1, 3).alias("status_prefix"))
    .orderBy("status_prefix", "store_id")
)
