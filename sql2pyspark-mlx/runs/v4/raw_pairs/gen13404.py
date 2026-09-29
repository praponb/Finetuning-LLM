from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy(F.quarter("signup_date").alias("signup_quarter"), "city")
    .agg(F.max("signup_date").alias("latest_signup_date"))
    .orderBy("latest_signup_date")
)
