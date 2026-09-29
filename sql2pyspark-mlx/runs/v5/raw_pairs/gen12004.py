from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").isNotNull())
    .agg(F.max("signup_date").alias("last_signup_date"))
    .orderBy(F.col("last_signup_date").desc())
)
