from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price")
    .union(df_subscriptions.select("monthly_fee"))
    .orderBy("unit_price")
)
