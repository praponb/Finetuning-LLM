from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country", F.month("signup_date").alias("signup_month"))
    .agg(F.max("signup_date").alias("latest_signup_date"))
    .orderBy(F.col("signup_month").desc(), "country")
)
