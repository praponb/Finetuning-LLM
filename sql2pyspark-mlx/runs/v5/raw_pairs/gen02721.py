from pyspark.sql import functions as F

recent_customers = df_customers.filter(F.col("segment").isin(["Consumer", "Corporate"]))

summary = (
    recent_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.count("credit_limit").alias("credit_limit_count"))
)

result = summary.select("signup_month", "credit_limit_count")
