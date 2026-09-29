from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.count("credit_limit").alias("credit_limit_count"))
    .orderBy(F.col("credit_limit_count").desc())
)
