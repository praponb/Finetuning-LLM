from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.year("signup_date") >= 2025)
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg((F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"))
    .filter(F.col("credit_limit_range") < 55)
)
