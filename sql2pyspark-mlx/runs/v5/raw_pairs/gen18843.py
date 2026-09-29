from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.size("tags") >= 2)
    .select(F.col("cost").alias("value"))
    .subtract(df_subscriptions.select(F.col("monthly_fee").alias("value")))
    .orderBy("value")
)
