from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "CANCELLED")
    .groupBy("channel")
    .agg(F.max("order_date").alias("latest_order_date"))
)
