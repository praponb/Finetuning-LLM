from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 8200)
    .groupBy("city", "segment")
    .agg(F.min("signup_date").alias("min_signup_date"))
    .orderBy(F.col("min_signup_date").desc(), F.col("segment").desc())
)
