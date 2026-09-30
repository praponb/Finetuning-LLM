from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date")
    .unionByName(df_products.select(F.col("launch_date").alias("end_date")))
)
