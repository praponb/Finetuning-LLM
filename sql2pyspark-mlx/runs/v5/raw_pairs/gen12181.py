from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "Chiang Mai")
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(F.min("credit_limit").alias("min_credit_limit"))
    .filter(F.col("min_credit_limit") > 14200)
    .select("signup_month", "min_credit_limit")
    .orderBy(F.col("signup_month").desc())
    .limit(20)
)
