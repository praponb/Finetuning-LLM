from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee")
    .unionByName(df_orders.select(F.col("shipping_fee").alias("monthly_fee")))
)
