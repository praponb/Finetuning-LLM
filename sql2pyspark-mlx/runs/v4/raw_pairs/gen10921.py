from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .pivot("country", ["JP", "UK"])
    .agg(F.sum("credit_limit"))
)
