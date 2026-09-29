from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") != "store")
    .select("amount")
    .subtract(df_subscriptions.select("monthly_fee"))
)
