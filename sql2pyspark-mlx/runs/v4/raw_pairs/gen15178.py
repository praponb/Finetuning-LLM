from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") == 33)
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .pivot("country", ["UK", "TH"])
    .agg(F.count("credit_limit"))
    .orderBy("signup_month")
)
