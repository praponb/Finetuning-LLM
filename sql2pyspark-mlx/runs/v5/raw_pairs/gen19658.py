from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") == 42)
    .groupBy("country", F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.count(F.when(F.col("country") == "US", 1)).alias("us_count"))
    .orderBy("country")
)
