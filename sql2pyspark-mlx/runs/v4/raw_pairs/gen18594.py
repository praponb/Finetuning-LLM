from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") != "Tokyo")
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(F.min("signup_date").alias("first_signup_date"))
    .orderBy("first_signup_date", "signup_month")
    .limit(10)
)
