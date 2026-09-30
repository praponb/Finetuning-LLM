from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([11, 15, 17, 18]))
    .select("order_date")
    .unionByName(df_subscriptions.select(F.col("start_date").alias("order_date")))
)
