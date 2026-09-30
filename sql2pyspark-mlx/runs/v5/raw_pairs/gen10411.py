from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") != "PAUSED")
    .select("monthly_fee")
    .intersect(df_orders.filter(F.col("amount").between(1060, 1470)).select("amount"))
)
