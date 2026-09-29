from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .unionByName(df_subscriptions.select(F.col("start_date").alias("signup_date")))
    .orderBy("signup_date")
)
