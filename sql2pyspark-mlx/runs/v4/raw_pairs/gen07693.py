from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.col("start_date").alias("event_date"))
    .unionByName(df_orders.select(F.col("order_date").alias("event_date")))
    .distinct()
)
