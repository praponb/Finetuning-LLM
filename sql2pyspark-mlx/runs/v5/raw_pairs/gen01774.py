from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 540)
    .select("order_date")
    .subtract(df_subscriptions.select("end_date"))
)
