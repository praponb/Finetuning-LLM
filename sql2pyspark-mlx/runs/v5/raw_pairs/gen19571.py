from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .subtract(df_subscriptions.select("monthly_fee"))
    .orderBy("shipping_fee")
)
