from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") >= "2024-09-22")
    .select("monthly_fee")
    .subtract(df_products.select("cost"))
)
