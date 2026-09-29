from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee")
    .unionByName(df_order_items.select(F.col("unit_price").alias("monthly_fee")))
    .orderBy("monthly_fee")
)
