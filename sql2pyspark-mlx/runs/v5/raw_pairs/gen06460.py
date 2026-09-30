from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "store")
    .select("shipping_fee")
    .subtract(df_subscriptions.select("monthly_fee"))
    .orderBy("shipping_fee")
)
