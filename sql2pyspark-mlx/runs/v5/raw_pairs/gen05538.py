from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.min("signup_date").alias("min_signup_date"))
    .orderBy(F.col("signup_quarter").desc())
    .limit(5)
)
