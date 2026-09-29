from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 13900)
    .agg(F.min("credit_limit").alias("min_credit_limit"))
)
