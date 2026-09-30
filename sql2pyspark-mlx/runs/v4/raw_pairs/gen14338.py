from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .unionByName(df_orders.select(F.col("order_date").alias("start_date")))
)
