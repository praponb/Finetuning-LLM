from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.count("credit_limit").alias("credit_limit_count"))
    .filter(F.col("credit_limit_count") > 4)
)
