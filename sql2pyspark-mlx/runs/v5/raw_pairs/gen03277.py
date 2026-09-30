from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .union(df_subscriptions.select("start_date"))
)
