from pyspark.sql import functions as F

result = (
    df_products
    .select("cost")
    .unionByName(df_subscriptions.select(F.col("monthly_fee").alias("cost")))
    .distinct()
)
