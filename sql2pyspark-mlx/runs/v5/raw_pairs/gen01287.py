from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "UK")
    .select("signup_date")
    .unionByName(df_subscriptions.select(F.col("start_date").alias("signup_date")))
    .orderBy(F.col("signup_date").desc())
)
