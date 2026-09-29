from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter((F.col("signup_date") >= "2023-09-07") & (F.col("credit_limit").between(6600, 15800) | F.col("is_active")))
)

customers_stats = (
    filtered_customers
    .groupBy("is_active")
    .agg(F.min("credit_limit").alias("min_credit_limit"))
)

result = customers_stats
