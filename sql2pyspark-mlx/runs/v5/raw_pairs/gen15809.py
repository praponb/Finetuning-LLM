from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("shipping_fee").alias("value"))
    .unionByName(df_subscriptions.select(F.col("monthly_fee").alias("value")))
    .distinct()
)
