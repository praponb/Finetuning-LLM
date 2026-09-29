from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") >= 710)
    .select("launch_date")
    .union(df_subscriptions.select("end_date"))
)
