from pyspark.sql import functions as F

result = (
    df_products
    .select("cost")
    .subtract(df_subscriptions.select("monthly_fee"))
)
