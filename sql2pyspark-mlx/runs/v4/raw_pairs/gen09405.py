from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") != "SMB")
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(F.min("signup_date").alias("min_signup_date"))
)
