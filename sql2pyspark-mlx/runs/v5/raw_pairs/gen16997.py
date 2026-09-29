from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id", "channel")
    .agg(F.count("discount").alias("discount_count"))
    .orderBy("store_id", "channel")
)
