from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .pivot("city", ["Berlin", "Singapore", "Chiang Mai", "New York"])
    .agg(F.max("age"))
)
