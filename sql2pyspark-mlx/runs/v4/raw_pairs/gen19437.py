from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 230)
    .select("monthly_fee")
    .union(df_orders.filter(F.col("shipping_fee") < 19).select("shipping_fee"))
    .distinct()
    .orderBy("monthly_fee")
)
