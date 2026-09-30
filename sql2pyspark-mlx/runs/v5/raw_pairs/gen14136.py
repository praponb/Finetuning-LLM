from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(9100, 12300))
    .withColumn("country_prefix", F.substring("country", 1, 3))
)
