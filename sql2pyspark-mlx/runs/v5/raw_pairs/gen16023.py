from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee")
    .union(df_products.select("price"))
    .distinct()
)
